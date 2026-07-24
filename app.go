package duro

import (
	"context"
	"errors"
	"log/slog"
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
}

// New initializes the application. Register pipelines and queues after New
// and before Launch.
func New(ctx context.Context, cfg Config) (*App, error) {
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
	dctx, err := dbos.NewDBOSContext(ctx, dbos.Config{
		AppName:     cfg.Name,
		DatabaseURL: cfg.DatabaseURL,
		Logger:      logger,
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
	return &App{DBOSContext: dctx, logger: logger}, nil
}

// Context returns the underlying DBOS context — for calling raw dbos package
// functions directly. Everything in duro accepts the App itself.
func (a *App) Context() Context { return a.DBOSContext }

// Launch starts DBOS: workflow recovery, queue runners, and schedulers. Call
// it after all registrations. It then warns about stranded runs — see App.
func (a *App) Launch() error {
	if err := dbos.Launch(a.DBOSContext); err != nil {
		return err
	}
	a.warnStranded()
	return nil
}

// Shutdown stops DBOS, waiting up to timeout for in-flight work to settle.
func (a *App) Shutdown(timeout time.Duration) {
	dbos.Shutdown(a.DBOSContext, timeout)
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
