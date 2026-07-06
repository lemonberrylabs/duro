// Package duro is a durable dataflow DSL: reactive pipelines whose every
// stage runs as a checkpointed DBOS step. Pipelines are powered by samber/ro
// internally, but the public API only accepts duro stages, making it a
// compile error to insert a raw ro operator that could break durability.
//
// A workflow body is a pipe of typed stages:
//
//	func OrderWorkflow(ctx dbos.DBOSContext, o Order) (Confirmation, error) {
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
	name  string
	kind  string
	apply func(ro.Observable[T]) ro.Observable[R]
}

func (s Stage[T, R]) info() stageInfo { return stageInfo{kind: s.kind, name: s.name} }

// StepOption configures how a durable stage executes as a DBOS step.
type StepOption func(*stepConfig)

type stepConfig struct {
	dbosOpts []dbos.StepOption
}

// WithMaxRetries sets the maximum number of automatic retries for the stage
// when its function returns an error. Zero (the default) means no retries.
func WithMaxRetries(n int) StepOption {
	return func(c *stepConfig) {
		c.dbosOpts = append(c.dbosOpts, dbos.WithStepMaxRetries(n))
	}
}

// WithBaseInterval sets the initial delay between retries (default 100ms).
func WithBaseInterval(d time.Duration) StepOption {
	return func(c *stepConfig) {
		c.dbosOpts = append(c.dbosOpts, dbos.WithBaseInterval(d))
	}
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

// runStep executes fn(in) as a named DBOS step after the runtime safety
// checks: the pipeline must have been subscribed by Run/RunAll, no earlier
// stage may have failed, and execution must still be on the subscribing
// goroutine.
func runStep[T, R any](ctx context.Context, name string, in T, fn func(context.Context, T) (R, error), opts []StepOption) (R, error) {
	var zero R

	state, _ := ctx.Value(pipelineStateKey{}).(*pipelineState)
	if state == nil {
		return zero, fmt.Errorf("duro: stage %q: pipeline was not started by duro.Run/RunAll inside a DBOS workflow", name)
	}
	if state.aborted.Load() {
		return zero, ErrAborted
	}
	if gid := goroutineID(); gid != state.gid {
		state.aborted.Store(true)
		return zero, fmt.Errorf("duro: stage %q executed on goroutine %d but the pipeline was subscribed on goroutine %d — a concurrent or time-based operator is present, which would break deterministic step ordering", name, gid, state.gid)
	}

	cfg := stepConfig{dbosOpts: []dbos.StepOption{dbos.WithStepName(name)}}
	for _, opt := range opts {
		opt(&cfg)
	}

	out, err := dbos.RunAsStep(state.dctx, func(stepCtx context.Context) (R, error) {
		return fn(stepCtx, in)
	}, cfg.dbosOpts...)
	if err != nil {
		state.aborted.Store(true)
	}
	return out, err
}
