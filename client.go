package duro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Client enqueues durable runs and reports their status without launching the
// engine — the abstraction for an enqueue-only process (a web tier) that starts
// workflows but must never execute them. It talks to the same system database
// as the workers, uses the same serialization, and reports status through the
// same mapping as the engine (Status/StatusAll), so a client and the workers
// can never disagree on what a run's state — or "terminal" — means.
//
//	c, err := duro.NewClient(ctx, duro.ClientConfig{DatabaseURL: url})
//	defer c.Shutdown(5 * time.Second)
//	h, err := duro.Enqueue(c, Jobs, InvoiceJob, batch)
//	status, err := c.Status(h.ID())
type Client struct {
	c      dbos.Client
	logger *slog.Logger
}

// ClientConfig configures a duro Client.
type ClientConfig struct {
	// DatabaseURL is the Postgres URL of the DBOS system database — the same one
	// the workers use.
	DatabaseURL string
	// Logger receives client and DBOS logs; slog.Default() when nil.
	Logger *slog.Logger
}

// NewClient connects an enqueue-only client to the system database.
func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.DatabaseURL == "" {
		return nil, errors.New("duro: ClientConfig.DatabaseURL is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	c, err := dbos.NewClient(ctx, dbos.ClientConfig{
		DatabaseURL: cfg.DatabaseURL,
		Logger:      logger,
	})
	if err != nil {
		return nil, err
	}
	return &Client{c: c, logger: logger}, nil
}

// Shutdown closes the client's system-database connection pool.
func (c *Client) Shutdown(timeout time.Duration) { c.c.Shutdown(timeout) }

// enqueueConfig accumulates EnqueueOptions.
type enqueueConfig struct {
	appVersion string
	workflowID string
}

// EnqueueOption configures a client Enqueue.
type EnqueueOption func(*enqueueConfig)

// WithClientApplicationVersion pins the enqueued run to a specific application
// version, so only workers on that version dequeue it. By default a client
// stamps no version (NULL), meaning any worker version may run it — the right
// default for a web tier that should not need redeploying in lockstep with the
// workers.
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
// By default no application version is stamped (any worker version may run it);
// override with WithClientApplicationVersion.
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
		// so any worker version may dequeue it. dbos otherwise defaults to the
		// client's own version — and the client, being a different binary from the
		// workers, would stamp a version no worker has, stranding every run.
		// WithClientApplicationVersion overrides this to pin a version.
		dbos.WithEnqueueApplicationVersion(cfg.appVersion),
	}
	if cfg.workflowID != "" {
		dopts = append(dopts, dbos.WithEnqueueWorkflowID(cfg.workflowID))
	}
	return newHandle(dbos.Enqueue[P, R](c.c, queue.Name(), job.Name(), input, dopts...))
}

// Status fetches a run's current status by workflow ID, using the same mapping
// as the engine's Status.
func (c *Client) Status(workflowID string) (RunStatus, error) {
	if workflowID == "" {
		return RunStatus{}, errors.New("duro: Status requires a workflow ID")
	}
	statuses, err := c.StatusAll(workflowID)
	if err != nil {
		return RunStatus{}, err
	}
	if len(statuses) == 0 {
		return RunStatus{}, fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
	}
	return statuses[0], nil
}

// StatusAll is the batch form of Status, sharing the engine's mapping core.
func (c *Client) StatusAll(workflowIDs ...string) ([]RunStatus, error) {
	return statusAll(c.c.ListWorkflows, workflowIDs)
}
