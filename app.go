package duro

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
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

// App owns the DBOS lifecycle so applications never touch it directly:
//
//	app, err := duro.New(ctx, duro.Config{Name: "orders", DatabaseURL: url})
//	wf := duro.Register(app, "invoice", invoicePipeline) // register everything...
//	err = app.Launch()                                   // ...then launch
//	defer app.Shutdown(5 * time.Second)
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
type App struct {
	dbos.DBOSContext
	logger *slog.Logger
	// wp holds the worker-pool liveness machinery; nil unless WithWorkerPool
	// (or WithRetention / WithStaleRunWarning) is passed to New.
	wp *workerPool
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
	dctx, err := dbos.NewDBOSContext(ctx, dbos.Config{
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
	if err := registerCancelWatcher(dctx); err != nil {
		dctx.Shutdown(5 * time.Second)
		return nil, err
	}
	app := &App{DBOSContext: dctx, logger: logger}
	// The worker-pool machinery (dedicated pool, heartbeat table) is also what
	// retention and stale-run warning run on, so open it for any of the three.
	if o.workerPool || o.retention > 0 || o.staleRunWarn > 0 {
		wp, err := newWorkerPool(ctx, cfg, dctx, o, logger)
		if err != nil {
			dctx.Shutdown(5 * time.Second)
			return nil, err
		}
		app.wp = wp
	}
	return app, nil
}

// Context returns the underlying DBOS context — for calling raw dbos package
// functions directly. Everything in duro accepts the App itself.
func (a *App) Context() Context { return a.DBOSContext }

// Launch starts DBOS: workflow recovery, queue runners, and schedulers. Call
// it after all registrations. It then warns about stranded runs — see App.
//
// In worker-pool mode Launch lands the first heartbeat synchronously before
// starting queue runners (an executor must never be dequeuing while observably
// dead), then starts the heartbeat and sweeper goroutines.
func (a *App) Launch() error {
	if a.wp != nil && a.wp.enabled {
		if err := a.wp.firstBeat(context.Background()); err != nil {
			return err
		}
	}
	if err := dbos.Launch(a.DBOSContext); err != nil {
		return err
	}
	if a.wp != nil {
		a.wp.start()
	}
	a.warnStranded()
	return nil
}

// Shutdown stops DBOS, waiting up to timeout for in-flight work to settle.
//
// In worker-pool mode the ordering matters: the sweeper stops first, the
// heartbeat keeps beating through DBOS's drain (so runs still executing here
// stay fresh and un-takeable however long the drain runs), and only once DBOS
// has stopped is the lease tombstoned — so any runs that could not drain are
// adopted by a survivor on its next sweep with no stale wait. Follow Shutdown
// promptly with process exit.
//
// timeout bounds the drain. The two steps around it are bounded separately and
// take milliseconds against a healthy database: stopping maintenance cancels
// whatever query it has in flight rather than waiting it out, and the tombstone
// is a single primary-key UPDATE bounded by a few seconds of its own. Budget
// timeout plus a small constant for the whole call — never let timeout consume
// the entire SIGTERM grace period, or the tombstone is the part that is lost.
//
// Calling Shutdown more than once is safe; the extra calls do nothing.
func (a *App) Shutdown(timeout time.Duration) {
	if a.wp != nil {
		a.wp.stopMaintenance()
	}
	dbos.Shutdown(a.DBOSContext, timeout)
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
}

// warnStranded logs every in-flight workflow whose recorded name is no longer
// registered — those runs can never be recovered by this executor, most
// commonly because a pipeline was renamed between deploys.
func (a *App) warnStranded() {
	registered, err := dbos.ListRegisteredWorkflows(a.DBOSContext)
	if err != nil {
		a.logger.Warn("duro: stranded-run check skipped: listing registered workflows", "error", err)
		return
	}
	active, err := dbos.ListWorkflows(a.DBOSContext,
		dbos.WithStatus([]dbos.WorkflowStatusType{
			dbos.WorkflowStatusPending,
			dbos.WorkflowStatusEnqueued,
			dbos.WorkflowStatusDelayed,
		}),
		dbos.WithLoadInput(false),
		dbos.WithLoadOutput(false),
		dbos.WithLimit(1000),
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
