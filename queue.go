package duro

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Queue is a declared DBOS workflow queue. Declare it once (package level is
// fine) and reference the value everywhere it is used — the queue's name
// lives in exactly one place, so writer and reader can never drift:
//
//	var Jobs = duro.NewQueue("jobs", duro.WithConcurrency(4))
//	...
//	duro.FanOut("process", Jobs, duro.Workflow(ProcessJob))
//
// Queues referenced by a pipeline are registered automatically when the
// pipeline is registered with Register; pipelines run directly with Run/RunAll
// (no Register) need RegisterQueues before workflows start enqueueing.
type Queue struct {
	name string
	cfg  queueConfig
	rt   *queueRuntime
}

// queueRuntime holds DBOS v1's database-backed handles separately from the
// immutable declaration. Queue values are copied freely throughout pipeline
// definitions, so all copies share this pointer.
type queueRuntime struct {
	mu        sync.RWMutex
	byRuntime map[*applicationRuntime]dbos.Queue
	// raw keeps registrations made against DBOS contexts that were not built
	// by Duro. Those contexts have no applicationRuntime value, so storing them
	// all under a nil runtime would let the last registration silently replace
	// every earlier handle.
	byContext map[Context]dbos.Queue
}

// queueConfig is a comparable snapshot of a queue's settings, so duro can
// detect two same-named declarations that disagree.
type queueConfig struct {
	globalConcurrency int // 0 = unlimited
	workerConcurrency int // 0 = unlimited
	rateLimit         int // 0 = no rate limit
	ratePeriod        time.Duration
	priorities        bool
	partitions        bool
}

// QueueOption configures a declared queue.
type QueueOption func(*queueConfig)

// WithConcurrency caps how many workflows from the queue run concurrently
// across all executors.
func WithConcurrency(n int) QueueOption {
	return func(c *queueConfig) { c.globalConcurrency = n }
}

// WithWorkerConcurrency caps how many workflows from the queue a single
// executor runs concurrently.
func WithWorkerConcurrency(n int) QueueOption {
	return func(c *queueConfig) { c.workerConcurrency = n }
}

// WithRateLimit caps how many workflows may start within each period —
// backpressure for external services.
func WithRateLimit(limit int, period time.Duration) QueueOption {
	return func(c *queueConfig) { c.rateLimit, c.ratePeriod = limit, period }
}

// WithPriorities enables priority scheduling: children enqueued with
// WithChildPriority run lowest-number-first.
func WithPriorities() QueueOption {
	return func(c *queueConfig) { c.priorities = true }
}

// WithPartitions makes the queue partitioned: children enqueued with
// WithChildPartitionKey get per-partition concurrency limits.
func WithPartitions() QueueOption {
	return func(c *queueConfig) { c.partitions = true }
}

// NewQueue declares a queue. Declaring is side-effect free; registration
// happens through Register (automatic for the pipeline's queues) or
// RegisterQueues.
func NewQueue(name string, opts ...QueueOption) Queue {
	if name == "" {
		panic("duro: NewQueue requires a non-empty name")
	}
	q := Queue{name: name, rt: &queueRuntime{
		byRuntime: make(map[*applicationRuntime]dbos.Queue),
		byContext: make(map[Context]dbos.Queue),
	}}
	for _, opt := range opts {
		opt(&q.cfg)
	}
	return q
}

// Name returns the queue's name.
func (q Queue) Name() string { return q.name }

func (q Queue) dbosOptions() []dbos.QueueOption {
	var opts []dbos.QueueOption
	if q.cfg.globalConcurrency > 0 {
		opts = append(opts, dbos.WithGlobalConcurrency(q.cfg.globalConcurrency))
	}
	if q.cfg.workerConcurrency > 0 {
		opts = append(opts, dbos.WithWorkerConcurrency(q.cfg.workerConcurrency))
	}
	if q.cfg.rateLimit > 0 {
		opts = append(opts, dbos.WithRateLimiter(&dbos.RateLimiter{Limit: q.cfg.rateLimit, Period: q.cfg.ratePeriod}))
	}
	if q.cfg.priorities {
		opts = append(opts, dbos.WithPriorityEnabled())
	}
	if q.cfg.partitions {
		opts = append(opts, dbos.WithPartitionQueue())
	}
	return opts
}

// queueLedgers tracks, per DBOS context, which queues duro has registered and
// with what config — so re-registering the same declaration is a no-op and
// two same-named declarations that disagree fail loudly instead of silently
// last-writer-wins in the database.
var queueLedgers sync.Map // applicationRegistryKey(ctx) → *queueLedger

type queueLedger struct {
	mu     sync.Mutex
	byName map[string]queueRegistration
}

type queueRegistration struct {
	cfg    queueConfig
	handle dbos.Queue
}

func (q Queue) remember(ctx Context, handle dbos.Queue) {
	if q.rt == nil {
		return
	}
	q.rt.mu.Lock()
	defer q.rt.mu.Unlock()
	if runtime := applicationRuntimeFromContext(ctx); runtime != nil {
		q.rt.byRuntime[runtime] = handle
		return
	}
	q.rt.byContext[ctx] = handle
}

func (q Queue) handle(ctx Context) (dbos.Queue, error) {
	if q.name == "" || q.rt == nil {
		return nil, errors.New("duro: queue was not built by NewQueue")
	}
	if ctx == nil {
		return nil, fmt.Errorf("duro: queue %q requires a non-nil context", q.name)
	}
	q.rt.mu.RLock()
	defer q.rt.mu.RUnlock()
	if runtime := applicationRuntimeFromContext(ctx); runtime != nil {
		if handle := q.rt.byRuntime[runtime]; handle != nil {
			return handle, nil
		}
	} else if handle := q.rt.byContext[ctx]; handle != nil {
		return handle, nil
	}
	// A raw dbos.Context does not carry Duro's application-name value. It is
	// still unambiguous when this declaration has exactly one registration.
	if len(q.rt.byRuntime)+len(q.rt.byContext) == 1 {
		for _, handle := range q.rt.byRuntime {
			return handle, nil
		}
		for _, handle := range q.rt.byContext {
			return handle, nil
		}
	}
	return nil, fmt.Errorf("duro: queue %q is not registered for application %q", q.name, applicationNameFromContext(ctx))
}

// WorkflowOption returns the DBOS workflow option that enqueues a directly
// started workflow on q. DBOS v1 requires the registered queue handle rather
// than its name. FanOut and Client.Enqueue resolve this automatically; use
// this method only with PipelineWorkflow.Start or RegisteredWorkflow.Start.
func (q Queue) WorkflowOption(ctx Context) (WorkflowOption, error) {
	handle, err := q.handle(unwrapContext(ctx))
	if err != nil {
		return nil, err
	}
	return dbos.WithQueue(handle), nil
}

// RegisterQueues registers declared queues with DBOS. Register does this
// automatically for every queue its pipeline references; call RegisterQueues
// yourself only for pipelines run directly with Run/RunAll inside hand-written
// workflows.
func RegisterQueues(ctx Context, queues ...Queue) error {
	for _, q := range queues {
		if err := ensureQueue(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// ensureQueue registers q once per context, failing on a conflicting
// same-named declaration.
func ensureQueue(ctx Context, q Queue) error {
	if q.name == "" || q.rt == nil {
		return errors.New("duro: queue was not built by NewQueue")
	}
	ctx = unwrapContext(ctx)
	if ctx == nil {
		return fmt.Errorf("duro: registering queue %q requires a non-nil context", q.name)
	}
	entry, _ := queueLedgers.LoadOrStore(applicationRegistryKey(ctx), &queueLedger{byName: make(map[string]queueRegistration)})
	ledger := entry.(*queueLedger)

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if existing, ok := ledger.byName[q.name]; ok {
		if existing.cfg != q.cfg {
			return fmt.Errorf("duro: queue %q declared twice with different configurations (%+v vs %+v) — declare it once and share the value", q.name, existing.cfg, q.cfg)
		}
		q.remember(ctx, existing.handle)
		return nil
	}
	handle, err := dbos.RegisterQueue(ctx, q.name, q.dbosOptions()...)
	if err != nil {
		return fmt.Errorf("duro: registering queue %q: %w", q.name, err)
	}
	ledger.byName[q.name] = queueRegistration{cfg: q.cfg, handle: handle}
	q.remember(ctx, handle)
	return nil
}
