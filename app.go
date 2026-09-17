package duro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures a duro application.
type Config struct {
	// Name identifies the application in the system database.
	Name string
	// DatabaseURL is the Postgres URL of the DBOS system database.
	DatabaseURL string
	// Logger receives duro and DBOS logs; slog.Default() when nil.
	Logger *slog.Logger

	// ApplicationVersion pins the DBOS application version — the single most
	// operationally consequential DBOS knob. Recovery is scoped to it: a run is
	// only ever recovered (or, under worker-pool mode, taken over) by an
	// executor on the same version. Leaving it empty keeps DBOS's default (a
	// hash of the binary), which changes on every rebuild — so every in-flight
	// run strands the moment you deploy new code, recoverable only by rolling
	// back to the exact previous binary. Pin it to a value you control (a git
	// SHA, a release tag) so a redeploy of the same logical version resumes
	// in-flight work. The DBOS__APPVERSION environment variable, when set,
	// overrides this field.
	ApplicationVersion string
	// ExecutorID sets this process's executor identity. Empty keeps DBOS's
	// default ("local"). Worker-pool mode (WithWorkerPool) requires a distinct
	// ID per process and assigns a unique one when this is empty. The
	// DBOS__VMID environment variable, when set, overrides this field.
	ExecutorID string
}

// applicationRuntime is the in-process identity of one App. It distinguishes
// Apps with the same durable application name against different databases
// when resolving DBOS v1's database-backed queue handles and Duro registries.
type applicationRuntime struct{ name string }

// applicationRuntimeContextKey carries that identity into workflow contexts
// without deriving the DBOS root. DBOS v1 deliberately strips root lifecycle
// state from derived contexts, so this value must be installed on the standard
// parent before NewContext.
type applicationRuntimeContextKey struct{}

func applicationRuntimeFromContext(ctx Context) *applicationRuntime {
	if ctx == nil {
		return nil
	}
	runtime, _ := ctx.Value(applicationRuntimeContextKey{}).(*applicationRuntime)
	return runtime
}

func applicationNameFromContext(ctx Context) string {
	if runtime := applicationRuntimeFromContext(ctx); runtime != nil {
		return runtime.name
	}
	return ""
}

// applicationRegistryKey gives every context derived from one Duro App the
// same registry identity. Raw DBOS contexts fall back to their concrete
// context identity because they do not carry Duro's runtime value.
func applicationRegistryKey(ctx Context) any {
	if runtime := applicationRuntimeFromContext(ctx); runtime != nil {
		return runtime
	}
	return ctx
}

// App owns the DBOS lifecycle so applications never touch it directly:
//
//	app, err := duro.New(ctx, duro.Config{Name: "orders", DatabaseURL: url})
//	wf := duro.Register(app, "invoice", invoicePipeline) // register everything...
//	err = app.Launch()                                   // ...then launch
//	defer app.Close(5 * time.Second)
//	handle, err := wf.Start(app, batch)
//
// Launch also checks for stranded runs: in-flight workflows recorded under
// names no longer registered (a renamed pipeline) are reported as warnings
// instead of silently never recovering.
//
// *App satisfies Context, so it can be passed wherever duro expects one.
// Calling raw dbos package functions directly is different: several inspect
// the concrete context type, so hand them Context() rather than the App
// itself.
type embeddedDBOSContext = dbos.Context

type App struct {
	embeddedDBOSContext
	// DBOSContext retains the v0 embedded field name for callers that accessed
	// it explicitly. New code should use Context().
	DBOSContext dbos.Context
	logger      *slog.Logger
	// wp holds the worker-pool liveness machinery; nil unless WithWorkerPool
	// (or WithRetention / WithStaleRunWarning) is passed to New.
	wp *workerPool

	// databaseURL and admin back Resume's guarded transition, which is
	// duro's own SQL and needs a connection DBOS does not expose. The pool is
	// opened on first use, and only when there is no worker-pool pool to share.
	databaseURL string
	adminOnce   sync.Once
	admin       *pgxpool.Pool
	adminErr    error

	launchStarted atomic.Bool
}

// New initializes the application. Register pipelines and queues after New
// and before Launch. Options enable optional subsystems — see WithWorkerPool,
// WithRetention, and WithStaleRunWarning.
func New(ctx context.Context, cfg Config, opts ...Option) (*App, error) {
	if cfg.Name == "" {
		return nil, errors.New("duro: Config.Name is required")
	}
	if cfg.DatabaseURL == "" {
		return nil, errors.New("duro: Config.DatabaseURL is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	o := defaultAppOptions()
	for _, opt := range opts {
		opt(&o)
	}
	// Cadence mistakes would otherwise surface in a background goroutine long
	// after New and Launch returned success — a ticker panic, or worse, a
	// liveness signal loose enough to take over live executors' runs.
	if err := o.validate(); err != nil {
		return nil, err
	}
	// Worker-pool mode requires a process-unique executor identity: two
	// processes sharing DBOS's "local" default would see each other as one
	// executor and never take over. An explicit Config.ExecutorID or DBOS__VMID
	// always wins.
	if o.workerPool && cfg.ExecutorID == "" && os.Getenv("DBOS__VMID") == "" {
		cfg.ExecutorID = generateExecutorID()
	}
	ctx = context.WithValue(ctx, applicationRuntimeContextKey{}, &applicationRuntime{name: cfg.Name})
	dctx, err := dbos.NewContext(ctx, dbos.Config{
		AppName:            cfg.Name,
		DatabaseURL:        cfg.DatabaseURL,
		Logger:             logger,
		ApplicationVersion: cfg.ApplicationVersion,
		ExecutorID:         cfg.ExecutorID,
	})
	if err != nil {
		return nil, err
	}
	// Every app carries duro's cancellation watcher (see WithCancelSiblings):
	// watchers are queued workflows, and any executor may dequeue or recover
	// one, so the registration must exist on every process.
	if err := registerCancelWatcher(dctx, cfg.Name); err != nil {
		_ = dbos.Shutdown(dctx, 5*time.Second)
		return nil, err
	}
	app := &App{embeddedDBOSContext: dctx, DBOSContext: dctx, logger: logger, databaseURL: cfg.DatabaseURL}
	// The worker-pool machinery (dedicated pool, heartbeat table) is also what
	// retention and stale-run warning run on, so open it for any of the three.
	if o.workerPool || o.retention > 0 || o.staleRunWarn > 0 {
		wp, err := newWorkerPool(ctx, cfg, dctx, o, logger)
		if err != nil {
			_ = dbos.Shutdown(dctx, 5*time.Second)
			return nil, err
		}
		app.wp = wp
	}
	return app, nil
}

// Context returns the underlying DBOS context — for calling raw dbos package
// functions directly. Everything in duro accepts the App itself.
func (a *App) Context() Context { return a.DBOSContext }

// sqlExec returns the executor Resume's transition runs on: the worker-pool
// pool when there is one, otherwise a small pool opened on first use.
func (a *App) sqlExec() (execFunc, error) {
	if a.wp != nil {
		return poolExec(a.wp.pool), nil
	}
	a.adminOnce.Do(func() {
		pcfg, err := pgxpool.ParseConfig(a.databaseURL)
		if err != nil {
			a.adminErr = fmt.Errorf("duro: parsing database URL: %w", err)
			return
		}
		pcfg.MaxConns = 2 // one statement at a time, on an operator's cadence
		a.admin, a.adminErr = pgxpool.NewWithConfig(context.Background(), pcfg)
	})
	if a.adminErr != nil {
		return nil, a.adminErr
	}
	return poolExec(a.admin), nil
}

// poolExec adapts a pgx pool to the execFunc the run-control cores take.
func poolExec(pool *pgxpool.Pool) execFunc {
	return func(ctx context.Context, sql string, args ...any) (int64, error) {
		tag, err := pool.Exec(ctx, sql, args...)
		if err != nil {
			return 0, err
		}
		return tag.RowsAffected(), nil
	}
}

// Launch starts DBOS: workflow recovery, queue runners, and schedulers. Call
// it after all registrations. It then warns about stranded runs — see App.
//
// In worker-pool mode Launch lands the first heartbeat synchronously before
// starting queue runners (an executor must never be dequeuing while observably
// dead), then starts the heartbeat and sweeper goroutines.
func (a *App) Launch() error {
	if !a.launchStarted.CompareAndSwap(false, true) {
		return errors.New("duro: App.Launch may only be called once")
	}
	if a.wp != nil && a.wp.enabled {
		if err := a.wp.firstBeat(context.Background()); err != nil {
			a.launchStarted.Store(false) // DBOS has not started; a transient beat may be retried.
			return err
		}
	}
	if err := dbos.Launch(a.DBOSContext); err != nil {
		// DBOS v1 launch failures are terminal and already shut down its own
		// context. Close Duro's pools and tombstone any first heartbeat too.
		_ = a.Close(5 * time.Second)
		return err
	}
	if err := ApplySchedules(a.DBOSContext); err != nil {
		_ = a.Close(5 * time.Second)
		return fmt.Errorf("duro: applying schedules after launch: %w", err)
	}
	if a.wp != nil {
		a.wp.start()
	}
	a.warnStranded()
	a.warnNotLatestVersion()
	return nil
}

// Close stops DBOS, waiting up to timeout for local workflow execution to
// unwind. Interrupted workflows remain PENDING for recovery; Close does not
// durably cancel them.
//
// In worker-pool mode the ordering matters: the sweeper stops first, the
// heartbeat keeps beating while DBOS unwinds (so runs still executing here
// stay fresh and un-takeable however long that takes), and only once DBOS has
// stopped is the lease tombstoned — so interrupted runs are
// adopted by a survivor on its next sweep with no stale wait. Follow Close
// promptly with process exit.
//
// timeout bounds each stage of DBOS shutdown in turn, not the call: DBOS waits
// up to timeout for its queue runner, then for in-flight workflows, then for
// the system-database pool. Cancelling the root context stops the first and
// the last in milliseconds against a healthy database, so the call costs what
// the workflows stage costs — the full timeout whenever a run is mid-step and
// does not return — though a database that hangs the pool close adds a second
// timeout. The two steps around DBOS are bounded separately and also take
// milliseconds against a healthy database: stopping maintenance cancels
// whatever query it has in flight rather than waiting it out, and the tombstone
// is a single primary-key UPDATE bounded by a few seconds of its own. Budget
// timeout plus a small constant for the whole call — never let timeout consume
// the entire SIGTERM grace period, or the tombstone is the part that is lost.
//
// Close returns an error naming what DBOS could not stop before timeout;
// "workflows" is the expected answer when a long run is mid-step, and that run
// stays PENDING. Calling Close again is safe: duro's own steps (stopping
// maintenance, the tombstone, its pools) run once. After a Close that returned
// nil the extra calls do nothing; after one that timed out, DBOS waits again,
// for up to timeout per stage — so do not pair a deferred Close with a second
// one on the signal path unless the grace period can pay for both.
func (a *App) Close(timeout time.Duration) error {
	if a.wp != nil {
		a.wp.stopMaintenance()
	}
	shutdownErr := dbos.Shutdown(a.DBOSContext, timeout)
	// Refuse to open the admin pool from here on, and close it if it exists.
	a.adminOnce.Do(func() { a.adminErr = errors.New("duro: app is shut down") })
	if a.admin != nil {
		a.admin.Close()
	}
	if a.wp != nil {
		a.wp.closeOnce.Do(func() {
			a.wp.stopBeat()
			if a.wp.enabled {
				if err := a.wp.tombstone(context.Background()); err != nil {
					a.logger.Warn("duro: worker-pool: tombstoning heartbeat on shutdown failed", "executor_id", a.wp.executorID, "error", err)
				}
			}
			a.wp.close()
		})
	}
	return shutdownErr
}

// Shutdown implements dbos.Context's v1 lifecycle method. Prefer Close for a
// direct App call; the leading Client argument exists for DBOS interface
// dispatch and is intentionally ignored.
func (a *App) Shutdown(_ dbos.Client, timeout time.Duration) error { return a.Close(timeout) }

// warnNotLatestVersion logs when this executor launched on an application
// version that is not the latest registered one — relaunching an older version
// string does that, since DBOS never re-dates a version it already knows. Such
// an executor never dequeues a run enqueued with no version, and nothing else
// reports it. With WithStaleRunWarning the check also repeats on the sweep
// cadence, for a newer version that registers later.
func (a *App) warnNotLatestVersion() {
	ctx, cancel := dbos.WithTimeout(a.DBOSContext, opTimeout)
	defer cancel()
	version := a.DBOSContext.GetApplicationVersion()
	latest, err := latestApplicationVersion(ctx, version)
	if err != nil {
		a.logger.Warn("duro: latest-version check skipped", "error", err)
		return
	}
	if latest != version {
		a.logger.Warn(notLatestVersionWarning, "application_version", version, "latest_version", latest)
	}
}

// warnStranded logs every in-flight workflow whose recorded name is no longer
// registered — those runs can never be recovered by this executor, most
// commonly because a pipeline was renamed between deploys.
func (a *App) warnStranded() {
	registered := dbos.ListRegisteredWorkflows(a.DBOSContext)
	active, err := dbos.ListWorkflows(a.DBOSContext,
		dbos.WithFilterStatus([]dbos.WorkflowStatusType{
			dbos.WorkflowStatusPending,
			dbos.WorkflowStatusEnqueued,
			dbos.WorkflowStatusDelayed,
		}...),
		dbos.WithFilterLoadInput(false),
		dbos.WithFilterLoadOutput(false),
		dbos.WithFilterLimit(1000),
	)
	if err != nil {
		a.logger.Warn("duro: stranded-run check skipped: listing workflows", "error", err)
		return
	}
	for name, ids := range strandedRuns(registered, active) {
		a.logger.Warn("duro: in-flight workflows are recorded under a name that is no longer registered and cannot recover on this executor — was the pipeline renamed?",
			"workflow_name", name, "count", len(ids), "workflow_ids", ids)
	}
}

// strandedRuns maps each unregistered workflow name to the in-flight run IDs
// recorded under it, mirroring how DBOS recovery resolves a run to code: by
// custom name or FQN, qualified with the instance config name when present.
func strandedRuns(registered []dbos.WorkflowRegistryEntry, active []dbos.WorkflowStatus) map[string][]string {
	known := make(map[string]bool)
	for _, e := range registered {
		for _, base := range []string{e.Name, e.FQN} {
			if base == "" {
				continue
			}
			known[base] = true
			if e.ConfigName != "" {
				known[base+"/"+e.ConfigName] = true
			}
		}
	}
	stranded := make(map[string][]string)
	for _, w := range active {
		lookup := w.Name
		if w.ConfigName != nil && *w.ConfigName != "" {
			lookup = w.Name + "/" + *w.ConfigName
		}
		if !known[lookup] {
			stranded[lookup] = append(stranded[lookup], w.ID)
		}
	}
	return stranded
}

// unwrapContext resolves an *App to its inner DBOS context. duro's entry
// points call it so the App can be passed anywhere a DBOS context is
// expected, while DBOS itself always receives its own concrete type.
func unwrapContext(ctx Context) Context {
	if a, ok := ctx.(*App); ok {
		return a.DBOSContext
	}
	return ctx
}
