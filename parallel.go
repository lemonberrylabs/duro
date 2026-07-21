package duro

import (
	"context"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/samber/ro"
)

// Parallel is a durable parallel Step: items execute fn concurrently as DBOS
// steps within the workflow process, at most maxConcurrent at a time
// (unbounded if maxConcurrent <= 0), and results are emitted downstream in
// input order once the stream completes.
//
// Parallel is the in-process, lightweight sibling of FanOut: no queue and no
// child workflows, just concurrent steps inside the current workflow. Use
// FanOut when work should distribute across processes, survive independently,
// or obey queue-level rate limits; use Parallel when a bounded burst of
// concurrent steps in this process is enough.
//
// Determinism is preserved because each step's ID is assigned on the workflow
// goroutine at launch time, in stream order (dbos.Go exists precisely for
// this), and outcomes are collected in that same order. On recovery, completed
// steps replay from their checkpoints without re-running fn.
//
// If any step fails, results for items before the failure are still emitted
// downstream (matching sequential fail-fast semantics), all remaining steps
// are drained, and the pipeline fails with the first error in input order.
func Parallel[T, R any](name string, maxConcurrent int, fn func(ctx context.Context, in T) (R, error), opts ...StepOption) Stage[T, R] {
	mustValidStage("Parallel", name, fn == nil)
	return Stage[T, R]{name: name, kind: "parallel", apply: func(source ro.Observable[T]) ro.Observable[R] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[R]) ro.Teardown {
			var outcomes []<-chan dbos.StepOutcome[R]
			var sem chan struct{}
			if maxConcurrent > 0 {
				sem = make(chan struct{}, maxConcurrent)
			}
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
					if sem != nil {
						sem <- struct{}{} // backpressure: wait for a slot, on the workflow goroutine
					}
					cfg := newStepConfig(name, opts)
					outcome, err := dbos.Go(state.dctx, stepBody(cfg, in, fn), cfg.dbosOpts...)
					if err != nil {
						if sem != nil {
							<-sem
						}
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: starting parallel step: %w", name, err))
						return
					}
					if sem == nil {
						outcomes = append(outcomes, outcome)
						return
					}
					// Forward the outcome and free the slot. Plumbing only —
					// no DBOS calls happen off the workflow goroutine.
					watched := make(chan dbos.StepOutcome[R], 1)
					go func() {
						watched <- <-outcome
						<-sem
					}()
					outcomes = append(outcomes, watched)
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if failed {
						return
					}
					// Drain every outcome first so no step outlives the
					// workflow, then emit in input order up to the first error.
					collected := make([]dbos.StepOutcome[R], len(outcomes))
					for i, ch := range outcomes {
						collected[i] = <-ch
					}
					for _, outcome := range collected {
						if outcome.Err != nil {
							if state, stateErr := stageState(ctx, name); stateErr == nil {
								state.aborted.Store(true)
							}
							fail(ctx, fmt.Errorf("duro: stage %q: %w", name, outcome.Err))
							return
						}
						dest.NextWithContext(ctx, outcome.Result)
					}
					dest.CompleteWithContext(ctx)
				},
			))

			return sub.Unsubscribe
		})
	}}
}
