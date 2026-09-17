package duro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client enqueues durable runs and reports their status without launching the
// engine — the abstraction for an enqueue-only process (a web tier) that starts
// workflows but must never execute them. It talks to the same system database
// as the workers, uses the same serialization, and reports status through the
// same mapping as the engine (Status/StatusAll), so a client and the workers
// can never disagree on what a run's state — or "terminal" — means.
//
//	c, err := duro.NewClient(ctx, duro.ClientConfig{DatabaseURL: url, ApplicationName: "orders"})
//	defer c.Shutdown(5 * time.Second)
//	h, err := duro.Enqueue(c, Jobs, InvoiceJob, batch)
//	status, err := c.Status(h.ID())
type Client struct {
	// c is the enqueue and management path. DBOS v1's NewClient returns its
	// unlaunched context behind the narrower Client interface.
	c dbos.Client
	// pool is shared by DBOS and Resume's guarded transition.
	pool *pgxpool.Pool
	// reads is the DBOS client's underlying, never-launched context. Every
	// public read or remediation call derives one bounded child of it, so a
	// database outage costs a call its timeout instead of a goroutine
	// forever. It is the same context type the engine's functions run on, so
	// the client and the workers share one code path exactly.
	reads       dbos.Context
	readTimeout time.Duration
	// bound is nil on the client NewClient returns; a WithContext view carries
	// the caller's context here and every read observes it as well.
	bound  context.Context
	logger *slog.Logger
}

// DefaultReadTimeout bounds a Client's reads and remediations when
// ClientConfig.ReadTimeout is zero.
const DefaultReadTimeout = 30 * time.Second

// ClientConfig configures a duro Client.
type ClientConfig struct {
	// DatabaseURL is the Postgres URL of the DBOS system database — the same one
	// the workers use.
	DatabaseURL string
	// ApplicationName scopes reads and writes to one DBOS application. It
	// should normally equal the workers' Config.Name. Empty retains DBOS's
	// nameless administrative mode, which can list and modify every application
	// sharing the database and enqueues unclaimed work for a queue owner to
	// adopt.
	ApplicationName string
	// Logger receives client and DBOS logs; slog.Default() when nil.
	Logger *slog.Logger
	// ReadTimeout bounds every read and remediation call — Status, StatusAll,
	// ListRuns, Steps, Cancel, Resume — at DefaultReadTimeout when zero. The
	// bound covers the whole call, however many queries it issues. DBOS
	// retries a failed database read indefinitely (backing off to 30s between
	// attempts) until its context ends, so without a bound an outage parks a
	// goroutine per call until the database returns; with one, each call
	// returns an error wrapping context.DeadlineExceeded at the deadline. Set
	// it below your request deadline and page ListRuns so no single call
	// needs longer. Negative is an error.
	//
	// It does not cover Enqueue or waiting on a Handle; those operations use
	// the client's lifecycle context rather than a per-call context.
	ReadTimeout time.Duration
}

// NewClient connects an enqueue-only client to the system database.
func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.DatabaseURL == "" {
		return nil, errors.New("duro: ClientConfig.DatabaseURL is required")
	}
	if cfg.ReadTimeout < 0 {
		return nil, fmt.Errorf("duro: ClientConfig.ReadTimeout must not be negative, got %v", cfg.ReadTimeout)
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout == 0 {
		readTimeout = DefaultReadTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	pool, err := newClientPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	c, err := dbos.NewClient(ctx, dbos.ClientConfig{
		SystemDBPool: pool,
		AppName:      cfg.ApplicationName,
		Logger:       logger,
	})
	if err != nil {
		pool.Close()
		return nil, err
	}
	// NewClient currently constructs the same concrete context as NewContext,
	// but intentionally returns its unlaunched Client subset. Retain the
	// context only to derive bounded management calls; never Launch it.
	reads, ok := c.(dbos.Context)
	if !ok {
		_ = dbos.Shutdown(c, 5*time.Second)
		return nil, errors.New("duro: DBOS client does not expose a context for bounded reads")
	}
	return &Client{c: c, pool: pool, reads: reads, readTimeout: readTimeout, logger: logger}, nil
}

// newClientPool opens the client's own pool, configured as DBOS configures
// its pools (lazy, bounded, tagged for pg_stat_activity).
func newClientPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("duro: parsing database URL: %w", err)
	}
	pcfg.MaxConns = 20
	pcfg.MinConns = 0
	pcfg.MaxConnLifetime = time.Hour
	pcfg.MaxConnIdleTime = 5 * time.Minute
	pcfg.ConnConfig.ConnectTimeout = 10 * time.Second
	if pcfg.ConnConfig.RuntimeParams == nil {
		pcfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	pcfg.ConnConfig.RuntimeParams["application_name"] = "duro-client"
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("duro: opening client pool: %w", err)
	}
	return pool, nil
}

// Shutdown aborts any read still in flight and closes the client's
// system-database connections. WithContext views share those connections, so
// Shutdown on any of them closes the client for all. Calling it more than
// once is safe.
func (c *Client) Shutdown(timeout time.Duration) error { return dbos.Shutdown(c.c, timeout) }

// WithContext returns a view of the client whose reads and remediations are
// bounded by ctx as well as by ReadTimeout — the seam for a request handler:
//
//	runs, err := c.WithContext(r.Context()).ListRuns(duro.WithLimit(50))
//
// A call cut short by ctx's deadline fails with an error wrapping
// context.DeadlineExceeded, one cut short by its cancellation with
// context.Canceled, so HTTP timeout classification sees the right cause.
//
// The view shares the client's connections and its enqueue path; Enqueue and
// Handle waits are not bounded by ctx. ctx must be non-nil.
func (c *Client) WithContext(ctx context.Context) *Client {
	if ctx == nil {
		panic("duro: Client.WithContext requires a non-nil context")
	}
	view := *c
	view.bound = ctx
	return &view
}

// readContext derives the bounded DBOS context one public call runs on —
// every query that call issues shares it: the read timeout, tightened by the
// WithContext caller's context when there is one.
func (c *Client) readContext() (dbos.Context, context.CancelFunc) {
	return boundContext(c.reads, c.bound, c.readTimeout)
}

// boundContext derives a child of parent that ends at the sooner of timeout
// and bound's deadline, and ends early if bound is cancelled. bound may be
// nil. A deadline surfaces as context.DeadlineExceeded and a cancellation as
// context.Canceled, whichever side it came from.
func boundContext(parent dbos.Context, bound context.Context, timeout time.Duration) (dbos.Context, context.CancelFunc) {
	if bound != nil {
		if deadline, ok := bound.Deadline(); ok {
			if until := time.Until(deadline); until < timeout {
				timeout = until
			}
		}
	}
	dctx, cancel := dbos.WithTimeout(parent, timeout)
	if bound == nil {
		return dctx, cancel
	}
	// The caller's deadline needs no propagating: dctx's own deadline is at
	// or before it, so expiry surfaces as context.DeadlineExceeded from dctx's
	// timer. Cancelling here on expiry would race that timer and could
	// relabel a deadline as a cancellation.
	stop := context.AfterFunc(bound, func() {
		if !errors.Is(bound.Err(), context.DeadlineExceeded) {
			cancel()
		}
	})
	return dctx, func() {
		stop()
		cancel()
	}
}

// storeOn is the runStore a public call runs on: the engine's adapter over
// the call's bounded context and the client's pool — one code path with the
// workers, so the two cannot report or treat runs differently.
func (c *Client) storeOn(dctx dbos.Context) runStore {
	return engineStore(dctx, poolExec(c.pool))
}

// enqueueConfig accumulates EnqueueOptions.
type enqueueConfig struct {
	appVersion string
	workflowID string
}

// EnqueueOption configures a client Enqueue.
type EnqueueOption func(*enqueueConfig)

// WithClientApplicationVersion pins the enqueued run to a specific application
// version, so only workers on that version dequeue it. By default a client
// stamps no version (NULL), which DBOS routes to the owning application's
// latest registered version — the right default for a web tier that should not
// need redeploying in lockstep with the workers. "Latest" is the version that
// registered most recently, and workers on any other version never dequeue
// such a run: a fleet relaunched on an older version string, or left behind by
// a process that registered a newer one, stops receiving Client traffic
// without an error. Launch warns when that is the case, and so does
// WithStaleRunWarning when it becomes the case later.
func WithClientApplicationVersion(version string) EnqueueOption {
	return func(c *enqueueConfig) { c.appVersion = version }
}

// WithClientWorkflowID assigns the run's workflow ID — the idempotency key.
// Enqueuing the same ID twice re-attaches to the first run instead of starting
// another.
func WithClientWorkflowID(id string) EnqueueOption {
	return func(c *enqueueConfig) { c.workflowID = id }
}

// Enqueue places a run on the queue for a worker to execute, and returns a
// handle to it. The job supplies the registered workflow name and both types,
// so the call cannot name a pipeline that does not exist, or disagree with it
// about the input or result type — register the same job on the workers with
// RegisterJob. The run is serialized identically to an engine-side enqueue, so
// workers decode it transparently.
//
// By default no application version is stamped, so DBOS routes the run to the
// owning application's latest registered version; override with
// WithClientApplicationVersion to pin an exact version.
func Enqueue[P, R any](c *Client, queue Queue, job Job[P, R], input P, opts ...EnqueueOption) (Handle[R], error) {
	if c == nil {
		return Handle[R]{}, errors.New("duro: Enqueue requires a non-nil client")
	}
	if job.Name() == "" {
		return Handle[R]{}, errors.New("duro: Enqueue requires a job built by NewJob")
	}
	if queue.Name() == "" {
		return Handle[R]{}, errors.New("duro: Enqueue requires a queue built by NewQueue")
	}
	var cfg enqueueConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	dopts := []dbos.EnqueueOption{
		// duro registers a pipeline as a configured instance keyed by its name,
		// and a worker resolves a dequeued run to that instance by config name —
		// so stamp the config name to match, or the run would never dispatch.
		dbos.WithEnqueueConfigName(job.Name()),
		// Stamp no application version by default (empty → NULL in the database),
		// so DBOS routes it to the owning application's latest registered version.
		// WithClientApplicationVersion overrides this to pin an exact version.
		dbos.WithEnqueueApplicationVersion(cfg.appVersion),
	}
	if cfg.workflowID != "" {
		dopts = append(dopts, dbos.WithEnqueueWorkflowID(cfg.workflowID))
	}
	return newHandle(dbos.Enqueue[R](c.c, queue.Name(), job.Name(), input, dopts...))
}

// Status fetches a run's current status by workflow ID, using the same mapping
// as the engine's Status.
func (c *Client) Status(workflowID string) (RunStatus, error) {
	dctx, done := c.readContext()
	defer done()
	return statusOne(c.storeOn(dctx), "Status", workflowID)
}

// StatusAll is the batch form of Status, sharing the engine's mapping core.
func (c *Client) StatusAll(workflowIDs ...string) ([]RunStatus, error) {
	dctx, done := c.readContext()
	defer done()
	return statusAll(c.storeOn(dctx), workflowIDs)
}

// ListRuns lists durable runs with the same options, mapping, and failed-run
// treatment as the engine's ListRuns. A named client sees its application plus
// migrated unclaimed rows by default; a nameless administrative client sees
// every application.
func (c *Client) ListRuns(opts ...ListOption) ([]RunStatus, error) {
	dctx, done := c.readContext()
	defer done()
	return listRunsWith(c.storeOn(dctx), opts)
}

// Steps lists a run's executed steps, as the engine's Steps does.
func (c *Client) Steps(workflowID string) ([]StepStatus, error) {
	dctx, done := c.readContext()
	defer done()
	return runSteps(c.storeOn(dctx), workflowID)
}

// Cancel stops a live run, as the engine's Cancel does.
func (c *Client) Cancel(workflowID string) error {
	dctx, done := c.readContext()
	defer done()
	return cancelRun(c.storeOn(dctx), workflowID)
}

// Resume revives a cancelled or retries-exceeded run, as the engine's Resume
// does — the same guarded transition, on the client's own connection. The
// run executes on a worker; the client never runs it.
func (c *Client) Resume(workflowID string) error {
	dctx, done := c.readContext()
	defer done()
	return resumeRun(c.storeOn(dctx), workflowID)
}
