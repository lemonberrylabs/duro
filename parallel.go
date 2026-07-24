package duro

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
// downstream (matching sequential fail-fast semantics), and the pipeline
// fails with the first error in input order. By default every remaining step
// is drained to completion first; opt into WithCancelSiblingSteps to cancel
// in-flight siblings and skip unstarted items instead, while still failing
// with the first genuine error.
func Parallel[T, R any](name string, maxConcurrent int, fn func(ctx context.Context, in T) (R, error), opts ...StepOption) Stage[T, R] {
	mustValidStage("Parallel", name, fn == nil)
	cancelSiblings := resolveCancelSiblings("Parallel", name, opts, true)
	return Stage[T, R]{name: name, kind: "parallel", apply: func(source ro.Observable[T]) ro.Observable[R] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[R]) ro.Teardown {
			var outcomes []<-chan dbos.StepOutcome[R]
			var sem chan struct{}
			if maxConcurrent > 0 {
				sem = make(chan struct{}, maxConcurrent)
			}
			failed := false

			// The sibling-failure signal: fired when a step's error escapes
			// its retries, it cancels in-flight step contexts (via AfterFunc
			// in the step body) and makes unstarted bodies skip fn.
			var signalCtx context.Context
			var fireSignal context.CancelCauseFunc
			if cancelSiblings {
				signalCtx, fireSignal = context.WithCancelCause(context.Background())
			}

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
					body := stepBody(cfg, in, fn)
					if cancelSiblings {
						body = cancellableBody(signalCtx, body)
					}
					outcome, err := dbos.Go(state.dctx, body, cfg.dbosOpts...)
					if err != nil {
						if sem != nil {
							<-sem
						}
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: starting parallel step: %w", name, err))
						return
					}
					if sem == nil && !cancelSiblings {
						outcomes = append(outcomes, outcome)
						return
					}
					// Forward the outcome; fire the failure signal on a
					// genuine error, then free the slot — so an item blocked
					// on the semaphore observes the signal at launch. Plumbing
					// only: no DBOS calls happen off the workflow goroutine.
					watched := make(chan dbos.StepOutcome[R], 1)
					go func() {
						result := <-outcome
						if cancelSiblings && result.Err != nil && !isSiblingCancellation(result.Err) {
							fireSignal(result.Err)
						}
						watched <- result
						if sem != nil {
							<-sem
						}
					}()
					outcomes = append(outcomes, watched)
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if failed {
						return
					}
					// Drain every outcome first so no step outlives the
					// workflow (cancelled siblings settle as soon as fn honors
					// its context), then emit in input order up to the first
					// error.
					collected := make([]dbos.StepOutcome[R], len(outcomes))
					for i, ch := range outcomes {
						collected[i] = <-ch
					}
					stageErr, errIndex := firstParallelError(collected, cancelSiblings)
					for i, outcome := range collected {
						if errIndex >= 0 && i >= errIndex {
							break
						}
						dest.NextWithContext(ctx, outcome.Result)
					}
					if stageErr != nil {
						if state, err := stageState(ctx, name); err == nil {
							state.aborted.Store(true)
						}
						fail(ctx, fmt.Errorf("duro: stage %q: %w", name, stageErr))
						return
					}
					dest.CompleteWithContext(ctx)
				},
			))

			return func() {
				sub.Unsubscribe()
				if fireSignal != nil {
					fireSignal(nil)
				}
			}
		})
	}}
}

// siblingCancellationMark is the replay-stable marker embedded in the error a
// cancelled or skipped Parallel step records instead of running its function.
// Recovery replays a failed step as a plain error carrying the recorded
// message, so marking by message substring classifies identically on the live
// run and on every replay.
const siblingCancellationMark = "duro: cancelled by a sibling step failure"

// isSiblingCancellation reports whether a step outcome error was manufactured
// by WithCancelSiblingSteps rather than returned by the step function on its
// own initiative.
func isSiblingCancellation(err error) bool {
	return err != nil && strings.Contains(err.Error(), siblingCancellationMark)
}

// cancellableBody wraps a step body for WithCancelSiblingSteps. Once the
// failure signal has fired, bodies that have not started skip the function
// entirely, and in-flight bodies have their context cancelled; both record
// the marker error so replay classifies them as cancellations, never as the
// stage's own failure.
func cancellableBody[R any](signalCtx context.Context, body func(context.Context) (R, error)) func(context.Context) (R, error) {
	return func(stepCtx context.Context) (R, error) {
		var zero R
		if signalCtx.Err() != nil {
			return zero, fmt.Errorf("%s (step skipped)", siblingCancellationMark)
		}
		runCtx, cancel := context.WithCancel(stepCtx)
		defer cancel()
		stop := context.AfterFunc(signalCtx, cancel)
		defer stop()
		out, err := body(runCtx)
		if err != nil && signalCtx.Err() != nil && errors.Is(err, context.Canceled) {
			return zero, fmt.Errorf("%s: %w", siblingCancellationMark, err)
		}
		return out, err
	}
}

// firstParallelError picks the stage's error from the drained outcomes: the
// first error in input order — except that with sibling cancellation enabled,
// manufactured cancellation errors never mask a genuine failure, so the first
// genuine error wins even when a cancelled sibling sits earlier in the input.
// The returned index is where downstream emission stops (the first error of
// any kind).
func firstParallelError[R any](collected []dbos.StepOutcome[R], cancelSiblings bool) (error, int) {
	errIndex := -1
	for i, outcome := range collected {
		if outcome.Err == nil {
			continue
		}
		if errIndex < 0 {
			errIndex = i
		}
		if !cancelSiblings || !isSiblingCancellation(outcome.Err) {
			return outcome.Err, errIndex
		}
	}
	if errIndex >= 0 {
		return collected[errIndex].Err, errIndex
	}
	return nil, -1
}
