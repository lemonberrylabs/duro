// Package duro is a durable dataflow DSL: reactive pipelines whose every
// stage runs as a checkpointed DBOS step. Pipelines are powered by samber/ro
// internally, but the public API only accepts duro stages, making it a
// compile error to insert a raw ro operator that could break durability.
//
// A workflow body is a pipe of typed stages:
//
//	func OrderWorkflow(ctx duro.Context, o Order) (Confirmation, error) {
//		return duro.Run(ctx, o, duro.Pipe4(
//			duro.Step("validate", validateOrder),
//			duro.Step("reserve", reserveInventory),
//			duro.Step("charge", chargePayment, duro.WithMaxRetries(3)),
//			duro.Step("notify", sendConfirmation),
//		))
//	}
//
// Durability relies on DBOS replay determinism: on recovery the workflow
// function re-executes and the Nth step call must be the same logical
// operation as in the original run. duro enforces this with three layers:
//
//   - Compile time: PipeN only accepts Stage values, which can only be built
//     by this package's constructors. Concurrent or time-based ro operators
//     cannot be expressed. Sources cannot be swapped either — Run feeds the
//     workflow input through ro.Of internally.
//   - Construction time: Run checkpoints the pipeline's shape (ordered stage
//     kinds and names) as a hidden first step named "duro.shape". If a
//     replay constructs a different shape — non-deterministic pipeline
//     construction, changed code — Run fails immediately instead of letting
//     stages read misaligned checkpoints.
//   - Execution time: every stage asserts it runs on the goroutine the
//     pipeline was subscribed on, failing fast if an operator smuggled in
//     concurrency; and once any stage fails, a shared abort flag prevents
//     items behind the failure from executing further stages (fail-fast,
//     like sequential workflow code).
//
// Escape hatches: Pure wraps a deterministic, side-effect-free transform that
// is NOT checkpointed (it re-executes on every replay — it must be pure), and
// UnsafeOperator admits an arbitrary ro operator with no safety guarantees
// beyond the runtime guards. Both participate in the shape fingerprint.
//
// For parallelism, use FanOut (child workflows on a DBOS queue — bounded,
// distributed, per-child durability) or Parallel (concurrent steps in-process
// via dbos.Go — lightweight, no queue). Both preserve replay determinism:
// work is spawned and awaited in stream order on the workflow goroutine.
//
// The rest of DBOS's workflow toolkit is available as stages: Delay (durable
// sleep), Send/Recv (durable mailbox messaging — external signals and
// human-in-the-loop pauses), SetEvent/GetEvent (progress events published and
// read durably), and ToStream/FromStream (durable streams written and drained
// durably). Messaging goes through typed channels — Topic, Event, Stream —
// declared once and referenced by both sides, so keys and payload types
// cannot drift; declare a channel with Portable() to serialize its payloads
// in DBOS's cross-language format.
//
// Control flow is durable too: Branch and Switch route each item through
// embedded pipelines by a checkpointed decision, Loop repeats a pipeline
// until a checkpointed verdict says done, Sub embeds a pipeline as one named
// stage, and Collect folds the stream into a slice. Embedded pipelines are
// part of the shape fingerprint.
//
// Pipelines are also registrable as first-class workflows: Register names a
// pipeline as a DBOS workflow, RegisterScheduled runs one on a cron schedule
// (typed Pipeline[time.Time, R]), and RegisterDebounced collapses bursts of
// triggers into a single run. ForkFromStage restarts a completed or failed
// run from a named stage — optionally onto a different application version.
package duro

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/samber/ro"
)

// Context is the durable execution context every workflow runs under: it
// carries the checkpoint state duro's stages record to. It is DBOS's context
// type under a duro name (a type alias), so it satisfies context.Context and
// remains directly usable with any dbos API — but declaring and running
// workflows never requires importing dbos:
//
//	func Process(ctx duro.Context, job Job) (Result, error)
type Context = dbos.DBOSContext

// WorkflowFunc is a hand-written durable workflow function, the kind
// dbos.RegisterWorkflow registers and Workflow adapts into a FanOut child.
// Registered pipelines (Register) never touch this type.
type WorkflowFunc[P, R any] = dbos.Workflow[P, R]

// ErrNoValue is returned by Run when the pipeline completes without emitting
// any value (for example, when a Filter stage drops every item).
var ErrNoValue = errors.New("duro: pipeline completed without emitting a value")

// ErrAborted marks stage executions skipped because an earlier stage already
// failed. It never surfaces from Run: the first failure is the pipeline's
// error, and ErrAborted only travels through already-terminated downstream
// observers.
var ErrAborted = errors.New("duro: pipeline aborted by an earlier stage failure")

// Stage is one typed pipeline segment. Stages are nominal: only this
// package's constructors can build them, which is what keeps arbitrary ro
// operators out of durable pipelines at compile time.
type Stage[T, R any] struct {
	name   string
	kind   string
	apply  func(ro.Observable[T]) ro.Observable[R]
	queues []Queue  // queues this stage enqueues onto (FanOut); Register auto-registers them
	nested []string // fingerprints of embedded pipelines (Branch/Switch/Loop/Sub)
}

func (s Stage[T, R]) info() stageInfo {
	return stageInfo{kind: s.kind, name: s.name, nested: s.nested}
}

// StepOption configures how a durable stage executes as a DBOS step.
type StepOption func(*stepConfig)

type stepConfig struct {
	dbosOpts []dbos.StepOption
	timeout  time.Duration
}

// stepOption lifts a DBOS step option into a duro StepOption.
func stepOption(o dbos.StepOption) StepOption {
	return func(c *stepConfig) { c.dbosOpts = append(c.dbosOpts, o) }
}

// newStepConfig resolves a stage's step options, naming the DBOS step after
// the stage. It is called per step execution, not at stage construction:
// dbos.RunAsStep appends to the option slice it receives, so a shared,
// pre-resolved slice would race between concurrent runs of the same pipeline.
func newStepConfig(name string, opts []StepOption) stepConfig {
	cfg := stepConfig{dbosOpts: []dbos.StepOption{dbos.WithStepName(name)}}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithMaxRetries sets the maximum number of automatic retries for the stage
// when its function returns an error. Zero (the default) means no retries.
// This is the step-level retry limit (DBOS's WithStepMaxRetries), distinct
// from workflow recovery attempts, which are configured at registration.
func WithMaxRetries(n int) StepOption { return stepOption(dbos.WithStepMaxRetries(n)) }

// WithBaseInterval sets the initial delay between retries (default 100ms).
func WithBaseInterval(d time.Duration) StepOption { return stepOption(dbos.WithBaseInterval(d)) }

// WithMaxInterval caps the delay between retries (default 5s).
func WithMaxInterval(d time.Duration) StepOption { return stepOption(dbos.WithMaxInterval(d)) }

// WithBackoffFactor sets the exponential multiplier applied to the retry
// delay after each attempt (default 2.0).
func WithBackoffFactor(factor float64) StepOption { return stepOption(dbos.WithBackoffFactor(factor)) }

// WithRetryPredicate restricts which errors are retried: when the stage
// function returns an error for which pred is false, the stage stops
// immediately with that error even if retries remain. Use it to spend
// retries on transient failures only.
func WithRetryPredicate(pred func(error) bool) StepOption {
	return stepOption(dbos.WithRetryPredicate(pred))
}

// WithTimeout bounds each execution attempt of the stage function: the step
// context is cancelled after d and the attempt fails with the context's
// error. Retries get a fresh deadline. DBOS has no native step timeout, so
// the deadline is enforced in-process per attempt — the stage function must
// honor context cancellation for the timeout to take effect. For a durable
// deadline on a whole pipeline, start its workflow from a context derived
// with dbos.WithTimeout; for child workflows, see WithChildTimeout.
func WithTimeout(d time.Duration) StepOption {
	return func(c *stepConfig) { c.timeout = d }
}

func mustValidStage(kind, name string, fnIsNil bool) {
	if name == "" {
		panic(fmt.Sprintf("duro: %s stage requires a non-empty name", kind))
	}
	if fnIsNil {
		panic(fmt.Sprintf("duro: %s stage %q requires a non-nil function", kind, name))
	}
}

// Step is a durable Map: it transforms each item by running fn as a
// checkpointed DBOS step. On recovery, completed executions are replayed from
// the database instead of re-running fn.
func Step[T, R any](name string, fn func(ctx context.Context, in T) (R, error), opts ...StepOption) Stage[T, R] {
	mustValidStage("Step", name, fn == nil)
	return Stage[T, R]{name: name, kind: "step", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (R, context.Context, error) {
		out, err := runStep(ctx, name, in, fn, opts)
		return out, ctx, err
	})}
}

// Tap is a durable side effect: fn runs as a checkpointed DBOS step and the
// item passes through unchanged.
func Tap[T any](name string, fn func(ctx context.Context, in T) error, opts ...StepOption) Stage[T, T] {
	mustValidStage("Tap", name, fn == nil)
	s := Step(name, func(ctx context.Context, in T) (T, error) {
		return in, fn(ctx, in)
	}, opts...)
	s.kind = "tap"
	return s
}

// Filter is a durable filter: the predicate runs as a checkpointed DBOS step,
// so effectful or non-deterministic predicates still replay consistently on
// recovery. Items for which the predicate returns false are dropped.
func Filter[T any](name string, pred func(ctx context.Context, in T) (bool, error), opts ...StepOption) Stage[T, T] {
	mustValidStage("Filter", name, pred == nil)
	return Stage[T, T]{name: name, kind: "filter", apply: ro.FlatMapWithContext(func(ctx context.Context, in T) ro.Observable[T] {
		keep, err := runStep(ctx, name, in, pred, opts)
		if err != nil {
			return ro.Throw[T](err)
		}
		if !keep {
			return ro.Empty[T]()
		}
		return ro.Of(in)
	})}
}

// Expand is a durable one-to-many transform (a flattening FlatMap): fn runs
// as a checkpointed DBOS step and each element of its result is emitted
// downstream in order.
func Expand[T, R any](name string, fn func(ctx context.Context, in T) ([]R, error), opts ...StepOption) Stage[T, R] {
	mustValidStage("Expand", name, fn == nil)
	return Stage[T, R]{name: name, kind: "expand", apply: ro.FlatMapWithContext(func(ctx context.Context, in T) ro.Observable[R] {
		outs, err := runStep(ctx, name, in, fn, opts)
		if err != nil {
			return ro.Throw[R](err)
		}
		return ro.FromSlice(outs)
	})}
}

// Reduce is a durable fold: each accumulation runs as a checkpointed DBOS
// step, and the final accumulator is emitted when the source completes.
func Reduce[T, A any](name string, fn func(ctx context.Context, acc A, in T) (A, error), seed A, opts ...StepOption) Stage[T, A] {
	mustValidStage("Reduce", name, fn == nil)
	return Stage[T, A]{name: name, kind: "reduce", apply: func(source ro.Observable[T]) ro.Observable[A] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[A]) ro.Teardown {
			acc := seed
			failed := false

			sub := source.SubscribeWithContext(subCtx, ro.NewObserverWithContext(
				func(ctx context.Context, in T) {
					if failed {
						return
					}
					next, err := runStep(ctx, name, in, func(stepCtx context.Context, item T) (A, error) {
						return fn(stepCtx, acc, item)
					}, opts)
					if err != nil {
						failed = true
						dest.ErrorWithContext(ctx, err)
						return
					}
					acc = next
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if failed {
						return
					}
					dest.NextWithContext(ctx, acc)
					dest.CompleteWithContext(ctx)
				},
			))

			return sub.Unsubscribe
		})
	}}
}

// Pure is a non-durable transform: fn is NOT checkpointed and re-executes on
// every replay, so it must be deterministic and side-effect free. Use it for
// cheap reshaping between durable stages; anything effectful or fallible
// belongs in Step.
func Pure[T, R any](name string, fn func(in T) R) Stage[T, R] {
	mustValidStage("Pure", name, fn == nil)
	return Stage[T, R]{name: name, kind: "pure", apply: ro.Map(fn)}
}

// UnsafeOperator admits an arbitrary ro operator into a durable pipeline.
// duro cannot guarantee replay determinism for it: the operator must be
// synchronous, order-preserving, and deterministic, or recovery will fail —
// loudly if step names misalign or execution changes goroutines, silently if
// identically-named steps swap positions. Prefer the safe constructors.
func UnsafeOperator[T, R any](name string, op func(ro.Observable[T]) ro.Observable[R]) Stage[T, R] {
	mustValidStage("UnsafeOperator", name, op == nil)
	return Stage[T, R]{name: name, kind: "unsafe", apply: op}
}

// pipelineState carries the workflow's DBOSContext, the subscribing
// goroutine, and a shared abort flag through the observable chain.
type pipelineState struct {
	dctx    dbos.DBOSContext
	gid     uint64
	aborted atomic.Bool
}

type pipelineStateKey struct{}

// stageState recovers the pipeline state from the context flowing through the
// observable chain and performs the runtime safety checks shared by all
// durable stages: the pipeline must have been subscribed by Run/RunAll, no
// earlier stage may have failed, and execution must still be on the
// subscribing goroutine.
func stageState(ctx context.Context, name string) (*pipelineState, error) {
	state, _ := ctx.Value(pipelineStateKey{}).(*pipelineState)
	if state == nil {
		return nil, fmt.Errorf("duro: stage %q: pipeline was not started by duro.Run/RunAll inside a DBOS workflow", name)
	}
	if state.aborted.Load() {
		return nil, ErrAborted
	}
	if gid := goroutineID(); gid != state.gid {
		state.aborted.Store(true)
		return nil, fmt.Errorf("duro: stage %q executed on goroutine %d but the pipeline was subscribed on goroutine %d — a concurrent or time-based operator is present, which would break deterministic step ordering", name, gid, state.gid)
	}
	return state, nil
}

// runStep executes fn(in) as a named DBOS step after the stageState safety
// checks.
func runStep[T, R any](ctx context.Context, name string, in T, fn func(context.Context, T) (R, error), opts []StepOption) (R, error) {
	var zero R

	state, err := stageState(ctx, name)
	if err != nil {
		return zero, err
	}

	cfg := newStepConfig(name, opts)
	out, err := dbos.RunAsStep(state.dctx, stepBody(cfg, in, fn), cfg.dbosOpts...)
	if err != nil {
		state.aborted.Store(true)
	}
	return out, err
}

// stepBody adapts fn(in) to a DBOS step body, enforcing the configured
// per-attempt timeout.
func stepBody[T, R any](cfg stepConfig, in T, fn func(context.Context, T) (R, error)) func(context.Context) (R, error) {
	return func(stepCtx context.Context) (R, error) {
		if cfg.timeout > 0 {
			var cancel context.CancelFunc
			stepCtx, cancel = context.WithTimeout(stepCtx, cfg.timeout)
			defer cancel()
		}
		return fn(stepCtx, in)
	}
}
