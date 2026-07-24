package duro

import (
	"context"
	"errors"
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
	timeout          time.Duration
	cancelSiblings   bool
	watchInterval    time.Duration
	watchIntervalSet bool
	static           []dbos.WorkflowOption
	perItem          []any // each entry is a func(T) dbos.WorkflowOption, asserted by FanOut
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

// WithCancelSiblings makes the first failed child cancel every sibling that
// is not yet in a terminal state — running (PENDING) and never-dequeued
// (ENQUEUED) children alike — instead of letting them run to completion in
// the background. Failure detection is order-independent: the stage watches
// all children while awaiting, so an early failure in a long batch cancels
// promptly rather than after every child ahead of it finishes. The stage
// still fails with the triggering child's error; cancelled siblings surface
// as CANCELLED but never mask it, on the live run and on recovery replay
// alike, so a Rescue around the stage always sees the original failure.
//
// Choose it when sibling work is worthless once the batch's outcome is
// decided (cost-bearing generation, paid API calls); leave the default when
// every completion has value on its own (cleanup or deletion fan-outs, where
// more finished children on the failure path is strictly better).
//
// Semantics that differ from the default, beyond cancellation itself:
//
//   - The stage's checkpoint layout changes: results are awaited and
//     recorded as one step named after the stage, instead of one
//     DBOS.getResult step per child. The option therefore participates in
//     the pipeline's shape fingerprint — toggling it while runs are in
//     flight trips the shape guard on recovery instead of misreading
//     checkpoints. Treat toggling like any other pipeline edit.
//   - On failure the fan-out fails as a unit: nothing is emitted
//     downstream. Without the option, results before the first failure in
//     input order are emitted before the stage fails.
//   - Detection begins once the stage starts awaiting (all children
//     enqueued). Enqueueing itself never blocks on child completion.
//
// Cancellation does not cascade: DBOS cancels exactly the sibling children,
// and a cancelled child stops at the start of its next step. Workflows the
// cancelled child had itself started — grandchildren of this stage,
// including a nested FanOut's children — keep running to completion. To
// bound that cost, give the child's own fan-outs WithCancelSiblings (covers
// grandchild failures) and WithChildTimeout (bounds grandchild lifetime);
// cancelling a whole tree from outside remains dbos.CancelWorkflows over
// the grandchild IDs.
//
// Cancellation is idempotent and re-issued on recovery: a parent that
// crashes mid-cancellation re-observes the failure when the stage resumes
// and cancels whatever is still live.
//
// Nor does cancellation depend on this process surviving the await:
// alongside the batch, the stage enqueues duro's cancellation watcher — an
// internal durable workflow (registered by New as "duro.cancel-watcher" on
// the internal "duro.cancel-watch" queue; both names are durable identities)
// that watches the same children and cancels redundantly. Any executor can
// dequeue or recover the watcher, so a failure is acted on even when the
// parent's executor dies mid-await. The stage requires the watcher to be
// registered — apps built with New always have it; a hand-rolled DBOS
// context without it fails the stage immediately with a clear error rather
// than silently weakening the guarantee.
func WithCancelSiblings() ChildOption {
	return func(c *childConfig) { c.cancelSiblings = true }
}

// WithCancelWatchInterval tunes how often the cancellation watcher polls
// child statuses (default 5s). The watcher is the backstop for when the
// process awaiting the fan-out dies — while that process lives, the stage's
// own await detects failures within 250ms regardless of this setting, and
// after cancellation is issued the watcher re-checks immediately rather than
// waiting out an interval. Lower it when orphaned spend must be cut faster
// after an executor loss; raise it to shave database load on very
// long-running batches. Recorded in the watcher's durable input, so a
// deploy that changes it affects new batches only.
//
// It requires WithCancelSiblings and a positive duration — FanOut panics at
// construction time otherwise.
func WithCancelWatchInterval(d time.Duration) ChildOption {
	return func(c *childConfig) { c.watchInterval, c.watchIntervalSet = d, true }
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
// error. By default, children queued behind it are independent durable
// workflows and run to completion in the background — the right call when
// every completion has value on its own (cleanup, deletion). When surviving
// siblings are wasted spend once the batch has failed, opt into
// WithCancelSiblings, which cancels every non-terminal sibling promptly
// while still failing the stage with the original error.
func FanOut[T, R any](name string, queue Queue, wf WorkflowRef[T, R], opts ...ChildOption) Stage[T, R] {
	mustValidStage("FanOut", name, wf == nil)
	if queue.name == "" {
		panic(fmt.Sprintf("duro: FanOut stage %q requires a queue built by NewQueue", name))
	}
	var cfg childConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.watchIntervalSet {
		if !cfg.cancelSiblings {
			panic(fmt.Sprintf("duro: FanOut stage %q: WithCancelWatchInterval requires WithCancelSiblings", name))
		}
		if cfg.watchInterval <= 0 {
			panic(fmt.Sprintf("duro: FanOut stage %q: WithCancelWatchInterval requires a positive duration", name))
		}
	}
	childOpts := resolveChildOpts(cfg, name, queue, wf)
	childWf := wf.dbosWorkflow()

	// The two modes checkpoint differently (per-child getResult steps vs one
	// assembled await step), so the option is part of the stage's identity:
	// toggling it trips the shape guard instead of misreading checkpoints.
	kind := "fanout"
	if cfg.cancelSiblings {
		kind = "fanout+cancel"
	}

	return Stage[T, R]{name: name, kind: kind, queues: []Queue{queue}, apply: func(source ro.Observable[T]) ro.Observable[R] {
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
					if cfg.cancelSiblings {
						if len(handles) == 0 {
							dest.CompleteWithContext(ctx)
							return
						}
						state, err := stageState(ctx, name)
						if err != nil {
							fail(ctx, err)
							return
						}
						ids := make([]string, len(handles))
						for i, handle := range handles {
							ids[i] = handle.GetWorkflowID()
						}
						// The watcher enqueue is a checkpointed child spawn:
						// one deterministic step, re-attached on replay. Any
						// executor can run it, so cancellation no longer
						// depends on this process surviving the await.
						interval := cfg.watchInterval
						if interval <= 0 {
							interval = defaultCancelWatchPollInterval
						}
						watch := cancelWatchInput{Stage: name, WorkflowIDs: uniqueWorkflowIDs(ids), PollInterval: interval}
						if _, err := dbos.RunWorkflow(state.dctx, cancelWatcher, watch, dbos.WithQueue(cancelWatchQueueName)); err != nil {
							state.aborted.Store(true)
							fail(ctx, fmt.Errorf("duro: stage %q: enqueueing the cancellation watcher (built the app with duro.New, which registers it?): %w", name, err))
							return
						}
						results, err := awaitCancellingSiblings[R](state.dctx, name, ids)
						if err != nil {
							state.aborted.Store(true)
							fail(ctx, err)
							return
						}
						for _, result := range results {
							dest.NextWithContext(ctx, result)
						}
						dest.CompleteWithContext(ctx)
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

// fanOutCancelPollInterval is how often a cancel-enabled FanOut checks child
// statuses while awaiting — the upper bound on how long a failure goes
// unnoticed before siblings are cancelled.
const fanOutCancelPollInterval = 250 * time.Millisecond

// The cancellation watcher: a duro-internal durable workflow enqueued
// alongside every cancel-enabled fan-out batch. It watches the same children
// the parent's await step watches and cancels on failure, redundantly and
// idempotently — but because it is a queued workflow, any executor can
// dequeue or recover it. That is what keeps cancellation from depending on
// the parent's executor staying alive: if the parent dies right after a
// child fails, the watcher still cancels the survivors. duro.New registers
// the watcher workflow and its queue on every app; both names are durable
// identities and must never change.
const (
	cancelWatcherName    = "duro.cancel-watcher"
	cancelWatchQueueName = "duro.cancel-watch"
	// The backstop cadence; the parent's await polls faster, and a stage can
	// tune this with WithCancelWatchInterval.
	defaultCancelWatchPollInterval = 5 * time.Second
)

// cancelWatchQueue is unbounded and duro-owned: watchers must never compete
// with (or deadlock behind) user workloads on the batch's own queue.
var cancelWatchQueue = NewQueue(cancelWatchQueueName)

// cancelWatchInput is the watcher's durable input. Fields are exported for
// serialization; the type itself stays internal.
type cancelWatchInput struct {
	Stage        string        `json:"stage"`
	WorkflowIDs  []string      `json:"workflow_ids"`
	PollInterval time.Duration `json:"poll_interval"`
}

// cancelWatcher is the watcher workflow body. All its reads and cancels go
// through a detached context, so the loop records no steps: a recovered
// watcher simply starts over, which is exactly the idempotent behavior
// cancellation needs.
func cancelWatcher(ctx dbos.DBOSContext, in cancelWatchInput) (string, error) {
	interval := in.PollInterval
	if interval <= 0 {
		interval = defaultCancelWatchPollInterval
	}
	detached := dbos.From(ctx, context.Background())
	if err := watchAndCancel(ctx, detached, in.Stage, in.WorkflowIDs, interval); err != nil {
		return "", err
	}
	return fmt.Sprintf("all %d children of stage %q terminal", len(in.WorkflowIDs), in.Stage), nil
}

// registerCancelWatcher wires the watcher workflow and its queue into an app
// context. Called by duro.New, before Launch, on every app — cancellation
// must be recoverable on every executor, whether or not this process runs
// cancel-enabled pipelines itself.
func registerCancelWatcher(ctx Context) error {
	dbos.RegisterWorkflow(ctx, cancelWatcher, dbos.WithWorkflowName(cancelWatcherName))
	return ensureQueue(ctx, cancelWatchQueue)
}

// awaitCancellingSiblings is the awaiting phase of a cancel-enabled FanOut,
// recorded as a single DBOS step named after the stage. The step body watches
// all children at once, cancels the survivors as soon as any child reaches a
// failed terminal state, and assembles the outcome: every result in input
// order, or the triggering child's error.
//
// One step, rather than one getResult step per child, is what keeps replay
// deterministic here: which siblings completed before cancellation reached
// them is a race, so per-child checkpoints would be recorded for a
// nondeterministic subset and misalign on recovery. The assembled step's
// checkpoint is the single source of truth — recovery either replays it, or
// (if the crash preceded the record) re-executes the body, which re-reads the
// children's terminal states and re-issues cancellation idempotently.
//
// All watching, cancelling, and result reads go through a detached DBOS
// context (the workflow's context re-rooted on a plain background context):
// the same system database, but outside the workflow, so none of it is
// checkpointed as workflow steps.
func awaitCancellingSiblings[R any](dctx dbos.DBOSContext, name string, ids []string) ([]R, error) {
	return dbos.RunAsStep(dctx, func(context.Context) ([]R, error) {
		detached := dbos.From(dctx, context.Background())
		if err := watchAndCancel(dctx, detached, name, ids, fanOutCancelPollInterval); err != nil {
			return nil, err
		}
		results := make([]R, 0, len(ids))
		var firstFailure, firstCancellation error
		for _, id := range ids {
			handle, err := dbos.RetrieveWorkflow[R](detached, id)
			if err != nil {
				return nil, fmt.Errorf("duro: stage %q: retrieving child workflow %s: %w", name, id, err)
			}
			result, err := handle.GetResult()
			if err != nil {
				wrapped := fmt.Errorf("duro: stage %q: child workflow %s: %w", name, id, err)
				if isAwaitedCancellation(err) {
					if firstCancellation == nil {
						firstCancellation = wrapped
					}
				} else if firstFailure == nil {
					firstFailure = wrapped
				}
				continue
			}
			results = append(results, result)
		}
		// Cancelled siblings never mask the failure that triggered the
		// cancellation; a cancellation is the stage's error only when no
		// child failed outright (external cancellation, child timeouts).
		if firstFailure != nil {
			return nil, firstFailure
		}
		if firstCancellation != nil {
			return nil, firstCancellation
		}
		return results, nil
	}, dbos.WithStepName(name))
}

// uniqueWorkflowIDs deduplicates the child ID list in order — deduplicated
// enqueues (DeduplicationReturnExisting, colliding WithChildID) can hand two
// items the same child.
func uniqueWorkflowIDs(ids []string) []string {
	unique := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	return unique
}

// watchAndCancel polls the children's statuses until every one is terminal,
// issuing one bulk cancellation the first time any child is seen in a failed
// terminal state (ERROR, CANCELLED, or recovery attempts exhausted). The bulk
// cancel is idempotent — terminal children are skipped — and is retried on
// the next poll if it fails, degrading to drain semantics rather than
// replacing the children's outcome with an infrastructure error. Both the
// parent's await step and the cancellation watcher run this loop; whichever
// observes the failure first cancels.
func watchAndCancel(ctx context.Context, detached dbos.DBOSContext, name string, ids []string, pollInterval time.Duration) error {
	unique := uniqueWorkflowIDs(ids)

	cancelIssued := false
	for {
		statuses, err := dbos.ListWorkflows(detached,
			dbos.WithWorkflowIDs(unique),
			dbos.WithLimit(len(unique)),
			dbos.WithLoadInput(false),
			dbos.WithLoadOutput(false),
		)
		if err != nil {
			return fmt.Errorf("duro: stage %q: watching child workflows: %w", name, err)
		}
		if len(statuses) != len(unique) {
			return fmt.Errorf("duro: stage %q: %d of %d child workflows missing from the system database while awaiting", name, len(unique)-len(statuses), len(unique))
		}
		terminal := 0
		anyFailed := false
		for _, status := range statuses {
			switch status.Status {
			case dbos.WorkflowStatusSuccess:
				terminal++
			case dbos.WorkflowStatusError, dbos.WorkflowStatusCancelled, dbos.WorkflowStatusMaxRecoveryAttemptsExceeded:
				terminal++
				anyFailed = true
			}
		}
		if anyFailed && !cancelIssued {
			if err := dbos.CancelWorkflows(detached, unique); err == nil {
				cancelIssued = true
				// Cancellation just moved every live sibling to a terminal
				// state — re-read immediately instead of waiting out a poll
				// interval, so a slow backstop cadence never delays settling.
				continue
			}
		}
		if terminal == len(unique) {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(pollInterval):
		}
	}
}

// isAwaitedCancellation reports whether a child result error means the child
// was cancelled rather than failed.
func isAwaitedCancellation(err error) bool {
	var dbosErr *dbos.DBOSError
	return errors.As(err, &dbosErr) && dbosErr.Code == dbos.AwaitedWorkflowCancelled
}
