package duro

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/samber/ro"
)

// ChildOption configures the child workflows a FanOut stage enqueues. Policy
// options (priority, delay, timeout, auth, serialization, version) apply
// uniformly to every child; identity options (workflow ID, deduplication ID,
// partition key) derive a per-child value from the stream item. Item-derived
// options are typed by the stage's item type — FanOut panics at construction
// time if they were built for a different type.
type ChildOption func(*childConfig)

type childConfig struct {
	timeout time.Duration
	static  []dbos.WorkflowOption
	perItem []any // each entry is a func(T) dbos.WorkflowOption, asserted by FanOut
}

// staticChild lifts an item-independent DBOS workflow option into a ChildOption.
func staticChild(o dbos.WorkflowOption) ChildOption {
	return func(c *childConfig) { c.static = append(c.static, o) }
}

// perItemChild lifts an item-derived DBOS workflow option into a ChildOption.
func perItemChild[T any](fn func(in T) dbos.WorkflowOption) ChildOption {
	return func(c *childConfig) { c.perItem = append(c.perItem, fn) }
}

// WithChildID derives each child's workflow ID from its item, making child
// runs idempotent under an application-level key (e.g. an order ID): starting
// the same pipeline twice re-attaches to the same children instead of
// spawning duplicates. Without it, child IDs derive from the parent's step
// counter, which is idempotent per parent run but not across runs.
func WithChildID[T any](fn func(in T) string) ChildOption {
	return perItemChild(func(in T) dbos.WorkflowOption { return dbos.WithWorkflowID(fn(in)) })
}

// WithChildDeduplicationID derives a queue deduplication ID from each item.
// While a child holding the ID is active on the queue, enqueueing another
// with the same ID is rejected — or returns the existing child's handle under
// dbos.DeduplicationPolicyReturnExisting (see WithChildDeduplicationPolicy).
func WithChildDeduplicationID[T any](fn func(in T) string) ChildOption {
	return perItemChild(func(in T) dbos.WorkflowOption { return dbos.WithDeduplicationID(fn(in)) })
}

// DeduplicationPolicy controls how a colliding child deduplication ID is
// handled; see WithChildDeduplicationPolicy.
type DeduplicationPolicy = dbos.DeduplicationPolicy

const (
	// DeduplicationReject (the default) fails the enqueue of a child whose
	// deduplication ID is already held by an active child.
	DeduplicationReject = dbos.DeduplicationPolicyReject
	// DeduplicationReturnExisting returns the existing child's handle
	// instead, so both items observe the first child's result.
	DeduplicationReturnExisting = dbos.DeduplicationPolicyReturnExisting
)

// WithChildDeduplicationPolicy sets how a colliding deduplication ID is
// handled (default DeduplicationReject).
func WithChildDeduplicationPolicy(policy DeduplicationPolicy) ChildOption {
	return staticChild(dbos.WithDeduplicationPolicy(policy))
}

// WithChildPartitionKey derives each child's queue partition key from its
// item. The queue must be registered with dbos.WithPartitionQueue; each
// partition then gets its own concurrency limits.
func WithChildPartitionKey[T any](fn func(in T) string) ChildOption {
	return perItemChild(func(in T) dbos.WorkflowOption { return dbos.WithQueuePartitionKey(fn(in)) })
}

// WithChildPriority sets every child's queue priority (lower runs first). The
// queue must be registered with dbos.WithPriorityEnabled.
func WithChildPriority(priority uint) ChildOption {
	return staticChild(dbos.WithPriority(priority))
}

// WithChildDelay delays each child's dequeue by d: children start in the
// DELAYED status and become runnable once the delay expires.
func WithChildDelay(d time.Duration) ChildOption {
	return staticChild(dbos.WithDelay(d))
}

// WithChildAppVersion pins children to a specific application version,
// overriding the parent's. This affects which executors recover them.
func WithChildAppVersion(version string) ChildOption {
	return staticChild(dbos.WithApplicationVersion(version))
}

// WithPortableChildren stores each child's inputs, step outputs, events,
// messages, and streams in DBOS's cross-language portable JSON format, so
// non-Go DBOS applications can read them.
func WithPortableChildren() ChildOption {
	return staticChild(dbos.WithPortableWorkflow())
}

// WithChildAuthenticatedUser records the authenticated user on every child
// workflow's status.
func WithChildAuthenticatedUser(user string) ChildOption {
	return staticChild(dbos.WithAuthenticatedUser(user))
}

// WithChildAuthenticatedRoles records the authenticated roles on every child
// workflow's status.
func WithChildAuthenticatedRoles(roles ...string) ChildOption {
	return staticChild(dbos.WithAuthenticatedRoles(roles))
}

// WithChildAssumedRole records the assumed role on every child workflow's
// status.
func WithChildAssumedRole(role string) ChildOption {
	return staticChild(dbos.WithAssumedRole(role))
}

// WithChildTimeout gives every child a durable workflow deadline of d from
// its enqueue time: the deadline is stored with the child's status, survives
// recovery, and cancels the child when it expires. A timed-out child fails
// the pipeline when its result is awaited.
func WithChildTimeout(d time.Duration) ChildOption {
	return func(c *childConfig) { c.timeout = d }
}

// WorkflowRef identifies a workflow FanOut can start: a registered pipeline
// (*PipelineWorkflow, which carries its own dispatch metadata) or a
// hand-written DBOS workflow wrapped with Workflow.
type WorkflowRef[T, R any] interface {
	dbosWorkflow() dbos.Workflow[T, R]
	runOptions() []dbos.WorkflowOption
}

// Workflow adapts a hand-written, dbos-registered workflow function into a
// WorkflowRef:
//
//	duro.FanOut("process", Jobs, duro.Workflow(ProcessJob))
func Workflow[T, R any](fn WorkflowFunc[T, R]) WorkflowRef[T, R] {
	if fn == nil {
		return nil
	}
	return workflowFuncRef[T, R]{fn: fn}
}

type workflowFuncRef[T, R any] struct {
	fn dbos.Workflow[T, R]
}

func (r workflowFuncRef[T, R]) dbosWorkflow() dbos.Workflow[T, R] { return r.fn }
func (r workflowFuncRef[T, R]) runOptions() []dbos.WorkflowOption { return nil }

// resolveChildOpts validates the config against the stage's item type and
// returns the per-item DBOS option builder. It panics on a type mismatch —
// construction time, like every other stage validation.
func resolveChildOpts[T, R any](cfg childConfig, name string, queue Queue, ref WorkflowRef[T, R]) func(in T) []dbos.WorkflowOption {
	perItem := make([]func(T) dbos.WorkflowOption, len(cfg.perItem))
	for i, raw := range cfg.perItem {
		fn, ok := raw.(func(in T) dbos.WorkflowOption)
		if !ok {
			panic(fmt.Sprintf("duro: FanOut stage %q: item-derived child option built for a different item type (%T, want func(%T) dbos.WorkflowOption)", name, raw, *new(T)))
		}
		perItem[i] = fn
	}
	fixed := append([]dbos.WorkflowOption{dbos.WithQueue(queue.name)}, ref.runOptions()...)
	fixed = append(fixed, cfg.static...)
	return func(in T) []dbos.WorkflowOption {
		opts := make([]dbos.WorkflowOption, 0, len(fixed)+len(perItem))
		opts = append(opts, fixed...)
		for _, fn := range perItem {
			opts = append(opts, fn(in))
		}
		return opts
	}
}

// FanOut is a durable parallel map: each item starts the referenced workflow
// as a child on the queue, and once the stream completes, results are awaited
// and emitted downstream in input order. Parallelism, rate limits, and
// distribution across processes are governed entirely by the queue's
// declaration:
//
//	var Jobs = duro.NewQueue("jobs", duro.WithConcurrency(4))
//	...
//	duro.Pipe3(
//		duro.Expand("explode", split),
//		duro.FanOut("process", Jobs, duro.Workflow(ProcessJob)),
//		duro.Reduce("merge", merge, seed),
//	)
//
// The child can be a hand-written DBOS workflow (wrap it with Workflow) or a
// registered pipeline (pass the *PipelineWorkflow directly). Child workflows
// are configured with ChildOptions — identity (WithChildID,
// WithChildDeduplicationID, WithChildPartitionKey), scheduling
// (WithChildPriority, WithChildDelay, WithChildTimeout), and metadata
// (WithChildAuthenticatedUser, WithChildAppVersion, WithPortableChildren).
//
// FanOut is the sanctioned form of concurrency inside a duro pipeline: it is
// deterministic because children are enqueued in stream order (child workflow
// IDs derive from the parent's step counter, so a recovered parent re-attaches
// to its children instead of spawning duplicates) and awaited in that same
// order (each result is checkpointed in the parent). Every child is itself a
// durable workflow.
//
// On the first child failure, FanOut fails the pipeline with that child's
// error. Children queued behind it are independent durable workflows and run
// to completion in the background; cancel them with dbos.CancelWorkflows if
// that is not what you want.
func FanOut[T, R any](name string, queue Queue, wf WorkflowRef[T, R], opts ...ChildOption) Stage[T, R] {
	mustValidStage("FanOut", name, wf == nil)
	if queue.name == "" {
		panic(fmt.Sprintf("duro: FanOut stage %q requires a queue built by NewQueue", name))
	}
	var cfg childConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	childOpts := resolveChildOpts(cfg, name, queue, wf)
	childWf := wf.dbosWorkflow()

	return Stage[T, R]{name: name, kind: "fanout", queues: []Queue{queue}, apply: func(source ro.Observable[T]) ro.Observable[R] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[R]) ro.Teardown {
			var handles []dbos.WorkflowHandle[R]
			var cancels []context.CancelFunc
			failed := false

			fail := func(ctx context.Context, err error) {
				failed = true
				dest.ErrorWithContext(ctx, err)
			}

			sub := source.SubscribeWithContext(subCtx, ro.NewObserverWithContext(
				func(ctx context.Context, in T) {
					if failed {
						return
					}
					state, err := stageState(ctx, name)
					if err != nil {
						fail(ctx, err)
						return
					}
					runCtx := state.dctx
					if cfg.timeout > 0 {
						// The deadline travels into the child's durable status
						// at enqueue; the cancel only releases the local timer,
						// so it is deferred to teardown to keep handles usable.
						var cancel context.CancelFunc
						runCtx, cancel = dbos.WithTimeout(runCtx, cfg.timeout)
						cancels = append(cancels, cancel)
					}
					handle, err := dbos.RunWorkflow(runCtx, childWf, in, childOpts(in)...)
					if err != nil {
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: enqueueing child workflow: %w", name, err))
						return
					}
					handles = append(handles, handle)
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if failed {
						return
					}
					for _, handle := range handles {
						state, err := stageState(ctx, name)
						if err != nil {
							fail(ctx, err)
							return
						}
						result, err := handle.GetResult()
						if err != nil {
							state.aborted.Store(true)
							fail(ctx, fmt.Errorf("duro: stage %q: child workflow %s: %w", name, handle.GetWorkflowID(), err))
							return
						}
						dest.NextWithContext(ctx, result)
					}
					dest.CompleteWithContext(ctx)
				},
			))

			return func() {
				sub.Unsubscribe()
				for _, cancel := range cancels {
					cancel()
				}
			}
		})
	}}
}
