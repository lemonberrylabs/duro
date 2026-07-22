package duro

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/ro"
)

// Control-flow combinators embed whole pipelines inside a stage, replacing
// the imperative code that used to require a hand-written workflow. They stay
// replay-deterministic the same way Filter does: every routing, looping, or
// rescue decision runs as a checkpointed step, so a recovered workflow
// re-reads the recorded decisions and walks exactly the same stage sequence.
// The embedded pipelines' shapes fold into the containing stage's
// fingerprint, so editing an arm or body still trips the shape guard.

// Case pairs a route key with the pipeline that handles it; see Switch.
type Case[T, R any] struct {
	key string
	p   Pipeline[T, R]
}

// When declares a Switch case.
func When[T, R any](key string, p Pipeline[T, R]) Case[T, R] {
	if key == "" {
		panic("duro: When requires a non-empty case key")
	}
	mustValidEmbedded("When", key, p)
	return Case[T, R]{key: key, p: p}
}

// Switch is durable multi-way dispatch: route runs as a checkpointed step,
// and each item flows through the case pipeline matching the returned key —
// on replay the recorded key routes the item down the same arm. A key with
// no matching case fails the pipeline. Applied per item on multi-item
// streams; every arm's outputs are emitted downstream in stream order.
func Switch[T, R any](name string, route func(ctx context.Context, in T) (string, error), cases ...Case[T, R]) Stage[T, R] {
	return dispatchStage("switch", name, route, cases, nil)
}

// Branch is durable two-way dispatch: the predicate runs as a checkpointed
// step and each item flows through then or els accordingly. Both arms must
// produce the same output type — the compiler holds routing honest.
func Branch[T, R any](name string, pred func(ctx context.Context, in T) (bool, error), then, els Pipeline[T, R], opts ...StepOption) Stage[T, R] {
	mustValidStage("Branch", name, pred == nil)
	mustValidEmbedded("Branch", name, then)
	mustValidEmbedded("Branch", name, els)
	route := func(ctx context.Context, in T) (string, error) {
		ok, err := pred(ctx, in)
		if err != nil {
			return "", err
		}
		if ok {
			return "then", nil
		}
		return "else", nil
	}
	return dispatchStage("branch", name, route, []Case[T, R]{{key: "then", p: then}, {key: "else", p: els}}, opts)
}

// dispatchStage is the shared core of Switch and Branch: checkpoint the
// routing decision, then run the chosen embedded pipeline for the item on
// the workflow goroutine.
func dispatchStage[T, R any](kind, name string, route func(ctx context.Context, in T) (string, error), cases []Case[T, R], opts []StepOption) Stage[T, R] {
	mustValidStage(kind, name, route == nil)
	if len(cases) == 0 {
		panic(fmt.Sprintf("duro: %s stage %q requires at least one case", kind, name))
	}
	byKey := make(map[string]Pipeline[T, R], len(cases))
	nested := make([]string, 0, len(cases))
	var queues []Queue
	for _, c := range cases {
		if _, dup := byKey[c.key]; dup {
			panic(fmt.Sprintf("duro: %s stage %q has duplicate case %q", kind, name, c.key))
		}
		byKey[c.key] = c.p
		nested = append(nested, c.key+"="+c.p.fingerprint())
		queues = append(queues, c.p.queues...)
	}
	return Stage[T, R]{name: name, kind: kind, nested: nested, queues: queues, apply: func(source ro.Observable[T]) ro.Observable[R] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[R]) ro.Teardown {
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
					key, err := runStep(ctx, name, in, route, opts)
					if err != nil {
						fail(ctx, err)
						return
					}
					arm, ok := byKey[key]
					if !ok {
						if state, stateErr := stageState(ctx, name); stateErr == nil {
							state.aborted.Store(true)
						}
						fail(ctx, fmt.Errorf("duro: stage %q: no case for route key %q", name, key))
						return
					}
					if !forwardSub(ctx, arm, in, dest) {
						failed = true
					}
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if !failed {
						dest.CompleteWithContext(ctx)
					}
				},
			))
			return sub.Unsubscribe
		})
	}}
}

// Loop durably repeats the body pipeline until the until predicate — a
// checkpointed step — reports done, then emits the final value. Each
// iteration feeds the body's last emitted value back in (a body that emits
// nothing drops the item, like Filter). On replay the recorded predicate
// verdicts reproduce the exact iteration count. Pair the body with Delay for
// durable polling. Applied per item on multi-item streams.
//
// Iterations are unbounded, and a durable loop is more durable than a bug
// deserves: a predicate that can never report done keeps checkpointing and
// resumes across restarts. Give the loop a natural bound (track attempts in
// T and fail past a limit), or stop a runaway run with dbos.CancelWorkflow.
func Loop[T any](name string, body Pipeline[T, T], until func(ctx context.Context, in T) (bool, error), opts ...StepOption) Stage[T, T] {
	mustValidStage("Loop", name, until == nil)
	mustValidEmbedded("Loop", name, body)
	return Stage[T, T]{name: name, kind: "loop", nested: []string{body.fingerprint()}, queues: body.queues, apply: func(source ro.Observable[T]) ro.Observable[T] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[T]) ro.Teardown {
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
					v := in
					for {
						outs, err := collectSub(ctx, body, v)
						if err != nil {
							fail(ctx, err)
							return
						}
						if len(outs) == 0 {
							return // the body dropped the item
						}
						v = outs[len(outs)-1]
						done, err := runStep(ctx, name, v, until, opts)
						if err != nil {
							fail(ctx, err)
							return
						}
						if done {
							dest.NextWithContext(ctx, v)
							return
						}
					}
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if !failed {
						dest.CompleteWithContext(ctx)
					}
				},
			))
			return sub.Unsubscribe
		})
	}}
}

// Rescue is a durable except block: it runs the embedded pipeline for each
// item and intercepts its failure. On success the embedded pipeline's
// emissions flow downstream unchanged. On failure — after the embedded
// stages' own retry policies are exhausted — handler runs as a checkpointed
// step and decides the outcome: return a fallback R and nil error to swallow
// (the fallback is emitted downstream and the outer pipeline continues), or
// return an error to fail the outer pipeline with it (transform, or return
// cause unchanged to rethrow). handler receives the item that entered the
// embedded pipeline, so a pass-through swallow is `return in, nil` when the
// types match. opts apply to the handler step, like Branch's route opts.
//
// A failed embedded run is treated as a unit: its partial emissions are
// discarded and only the handler's fallback is emitted. A successful embedded
// run that emits nothing drops the item, like Filter. Because the handler is
// a checkpointed step, effectful handlers (failure reports, warn logs) replay
// consistently, and the rescue decision is durable: recovery never flips a
// swallowed failure into a propagated one or vice versa. The cause is
// replay-stable by construction: the handler always receives a plain error
// carrying exactly the terminal failure's message — the same value a
// recovered run replays — so the handler cannot behave differently on
// recovery than it did live. Match causes by message; error identities
// (errors.Is/As) are deliberately not preserved, because they cannot survive
// recovery. A decision that needs the typed error belongs in the failing
// step itself or its WithRetryPredicate, which always see the live error.
//
// Applied per item on multi-item streams. Only failures inside the embedded
// pipeline are rescued: upstream failures, and errors from the handler
// itself, propagate as usual. Rescue nests — the innermost enclosing Rescue
// wins — and Pipe1(Rescue(name, p, handler)) is the whole-pipeline except
// block.
func Rescue[T, R any](name string, p Pipeline[T, R], handler func(ctx context.Context, in T, cause error) (R, error), opts ...StepOption) Stage[T, R] {
	mustValidStage("Rescue", name, handler == nil)
	mustValidEmbedded("Rescue", name, p)
	return Stage[T, R]{name: name, kind: "rescue", nested: []string{p.fingerprint()}, queues: p.queues, apply: func(source ro.Observable[T]) ro.Observable[R] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[R]) ro.Teardown {
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
					// The embedded run gets its own abort flag: Rescue is the
					// one combinator that continues past an embedded failure,
					// so the abort it triggers must not leak into the outer
					// pipeline and turn every later stage into ErrAborted.
					child := &pipelineState{dctx: state.dctx, gid: state.gid}
					outs, cause := collectSub(context.WithValue(ctx, pipelineStateKey{}, child), p, in)
					if cause == nil {
						for _, out := range outs {
							dest.NextWithContext(ctx, out)
						}
						return
					}
					cause = rescueCause(cause)
					out, err := runStep(ctx, name, in, func(stepCtx context.Context, item T) (R, error) {
						return handler(stepCtx, item, cause)
					}, opts)
					if err != nil {
						fail(ctx, err)
						return
					}
					dest.NextWithContext(ctx, out)
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if !failed {
						dest.CompleteWithContext(ctx)
					}
				},
			))
			return sub.Unsubscribe
		})
	}}
}

// Sub embeds a pipeline as a single named stage — reuse a pipeline segment
// across pipelines without a wrapper workflow. Unlike Branch/Switch/Loop,
// Sub applies to the whole stream, not per item: an embedded Reduce folds
// everything flowing through it.
func Sub[T, R any](name string, p Pipeline[T, R]) Stage[T, R] {
	if name == "" {
		panic("duro: Sub stage requires a non-empty name")
	}
	mustValidEmbedded("Sub", name, p)
	return Stage[T, R]{name: name, kind: "sub", nested: []string{p.fingerprint()}, queues: p.queues, apply: p.apply}
}

// Via runs the embedded pipeline for each item — durably, to completion —
// then emits the ORIGINAL item downstream, discarding the embedded
// pipeline's emissions. Tap is to Step what Via is to Sub: the embedded
// pipeline exists for its effects (typically a FanOut of child workflows or
// a Parallel fleet), not its outputs, and the item that entered continues on
// afterwards. That is what lets a pipeline whose state must survive a
// fan-out stay a single registered pipeline instead of a hand-written
// workflow around several Run calls.
//
// Via never drops items: a successful embedded run that emits nothing (a
// Filter dropped everything) still passes the original item through —
// embedded emissions are ignored entirely, zero included. This is the
// deliberate contrast with Sub, whose emissions ARE the stream. The
// re-emitted item is replay-stable because it came from upstream
// checkpoints; Via itself records no step.
//
// An embedded failure fails the outer pipeline exactly like any stage
// failure — partial embedded effects before it remain checkpointed. Wrap in
// Rescue to swallow: Rescue(name, Pipe1(Via(...)), handler) is a best-effort
// fan-out that continues with the original item either way. Applied per item
// on multi-item streams, in stream order.
func Via[T, R any](name string, p Pipeline[T, R]) Stage[T, T] {
	if name == "" {
		panic("duro: Via stage requires a non-empty name")
	}
	mustValidEmbedded("Via", name, p)
	return Stage[T, T]{name: name, kind: "via", nested: []string{p.fingerprint()}, queues: p.queues, apply: ro.FlatMapWithContext(func(ctx context.Context, in T) ro.Observable[T] {
		if _, err := collectSub(ctx, p, in); err != nil {
			return ro.Throw[T](err)
		}
		return ro.Of(in)
	})}
}

// Collect folds the stream into a slice of every item, in order — the
// standard final stage for a registered pipeline that should return all
// values rather than the last one. An empty stream yields an empty slice.
func Collect[T any](name string, opts ...StepOption) Stage[T, []T] {
	s := Reduce(name, func(_ context.Context, acc []T, v T) ([]T, error) {
		return append(acc, v), nil
	}, nil, opts...)
	s.kind = "collect"
	return s
}

// rescueCause normalizes an embedded failure to its replay-stable form
// before the handler sees it. A recovered run replays a failed step as a
// plain error carrying only the recorded message (DBOS stores err.Error(),
// or its portable equivalent whose Error() returns the same message), so the
// live path strips the error down to that same value: a handler can never
// observe a difference between a live failure and a replayed one.
func rescueCause(err error) error { return errors.New(err.Error()) }

// forwardSub routes one item through an embedded pipeline on the current
// goroutine, forwarding its emissions downstream; reports success.
func forwardSub[T, R any](ctx context.Context, p Pipeline[T, R], in T, dest ro.Observer[R]) bool {
	ok := true
	p.apply(ro.Of(in)).SubscribeWithContext(ctx, ro.NewObserverWithContext(
		func(c context.Context, out R) { dest.NextWithContext(c, out) },
		func(c context.Context, err error) {
			ok = false
			dest.ErrorWithContext(c, err)
		},
		func(context.Context) {}, // per-item completion; the outer stream continues
	)).Unsubscribe()
	return ok
}

// collectSub routes one item through an embedded pipeline on the current
// goroutine and gathers its emissions.
func collectSub[T, R any](ctx context.Context, p Pipeline[T, R], in T) ([]R, error) {
	var outs []R
	var failure error
	p.apply(ro.Of(in)).SubscribeWithContext(ctx, ro.NewObserverWithContext(
		func(_ context.Context, out R) { outs = append(outs, out) },
		func(_ context.Context, err error) { failure = err },
		func(context.Context) {},
	)).Unsubscribe()
	return outs, failure
}

// mustValidEmbedded panics when a control-flow stage is given a zero-value
// pipeline.
func mustValidEmbedded[P, R any](kind, name string, p Pipeline[P, R]) {
	if p.apply == nil {
		panic(fmt.Sprintf("duro: %s %q requires pipelines built by Pipe1..Pipe8", kind, name))
	}
}
