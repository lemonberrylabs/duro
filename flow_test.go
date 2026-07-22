package duro_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- control-flow test workflows ---------------------------------------------

var (
	halveRuns  atomic.Int64
	tripleRuns atomic.Int64
	loopRuns   atomic.Int64
)

var (
	halvePipe = duro.Pipe1(duro.Step("halve", func(_ context.Context, v int) (int, error) {
		halveRuns.Add(1)
		return v / 2, nil
	}))
	triplePipe = duro.Pipe1(duro.Step("triple", func(_ context.Context, v int) (int, error) {
		tripleRuns.Add(1)
		return v * 3, nil
	}))
)

// branchWorkflow routes each item by parity: evens halve, odds triple.
func branchWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Branch("route", func(_ context.Context, v int) (bool, error) {
			return v%2 == 0, nil
		}, halvePipe, triplePipe),
		duro.Collect[int]("collect"),
	))
}

// switchWorkflow dispatches on a string key; "boom" routes to no case.
func switchWorkflow(ctx dbos.DBOSContext, kinds []string) ([]string, error) {
	return duro.Run(ctx, kinds, duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, ks []string) ([]string, error) {
			return ks, nil
		}),
		duro.Switch("dispatch", func(_ context.Context, k string) (string, error) {
			return k, nil
		},
			duro.When("upper", duro.Pipe1(duro.Step("up", func(_ context.Context, k string) (string, error) {
				return strings.ToUpper(k), nil
			}))),
			duro.When("tag", duro.Pipe1(duro.Pure("tagged", func(k string) string {
				return "#" + k
			}))),
		),
		duro.Collect[string]("collect"),
	))
}

// loopWorkflow increments until the value reaches 4: do-while durable loop.
func loopWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Loop("until-four", duro.Pipe1(duro.Step("inc", func(_ context.Context, v int) (int, error) {
			loopRuns.Add(1)
			return v + 1, nil
		})), func(_ context.Context, v int) (bool, error) {
			return v >= 4, nil
		}),
	))
}

// subWorkflow embeds a whole-stream fold: Sub is not per-item.
func subWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	sum := duro.Pipe1(duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
		return acc + v, nil
	}, 0))
	return duro.Run(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.Sub("total", sum),
	))
}

// collectEmptyWorkflow proves Collect turns an empty stream into an empty
// slice instead of ErrNoValue.
func collectEmptyWorkflow(ctx dbos.DBOSContext, n int) ([]int, error) {
	return duro.Run(ctx, n, duro.Pipe2(
		duro.Filter("drop-all", func(_ context.Context, _ int) (bool, error) { return false, nil }),
		duro.Collect[int]("collect"),
	))
}

// includeExtraArmStage simulates editing a Branch arm between the original
// run and a replay, for the nested shape-guard test.
var includeExtraArmStage = false

func mutableBranchWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	then := duro.Pipe1(duro.Step("double", func(_ context.Context, v int) (int, error) { return v * 2, nil }))
	if includeExtraArmStage {
		then = duro.Pipe2(
			duro.Step("double", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
			duro.Step("extra", func(_ context.Context, v int) (int, error) { return v + 1, nil }),
		)
	}
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Branch("route", func(_ context.Context, _ int) (bool, error) { return true, nil },
			then,
			duro.Pipe1(duro.Pure("as-is", func(v int) int { return v })),
		),
	))
}

// loopDropWorkflow's body filters its item away: the loop must end without
// emitting, and Run reports ErrNoValue.
func loopDropWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Loop("drop-loop", duro.Pipe1(
			duro.Filter("drop", func(_ context.Context, _ int) (bool, error) { return false, nil }),
		), func(_ context.Context, _ int) (bool, error) { return true, nil }),
	))
}

var armAfterFailureRuns atomic.Int64

// branchFailureWorkflow fails inside the then-arm on item 2.
func branchFailureWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	arm := duro.Pipe1(duro.Step("check", func(_ context.Context, v int) (int, error) {
		if v == 2 {
			return 0, errors.New("boom: two is forbidden")
		}
		armAfterFailureRuns.Add(1)
		return v, nil
	}))
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Branch("route", func(_ context.Context, _ int) (bool, error) { return true, nil },
			arm,
			duro.Pipe1(duro.Pure("as-is", func(v int) int { return v })),
		),
		duro.Collect[int]("collect"),
	))
}

var (
	rescuePostRuns            atomic.Int64
	rescueRethrowSurvivorRuns atomic.Int64
	rescueUpstreamHandlerRuns atomic.Int64
	rescueRetryAttempts       atomic.Int64
	rescueCrashBoomRuns       atomic.Int64
	rescueCrashHandlerRuns    atomic.Int64
	rescueCrashObservedCause  atomic.Value // string: cause message seen by the crash handler
	rescueCrashSawSentinel    atomic.Bool  // whether errors.Is matched the sentinel in the handler
)

// rescueSwallowWorkflow is the abort-containment shape: outer stage →
// Rescue(failing inner) → outer stage. Item 2 fails inside the rescued
// pipeline; the handler swallows with a negated fallback, and the trailing
// stages must keep executing for it and for the items behind it.
func rescueSwallowWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	boom := duro.Pipe1(duro.Step("boom", func(_ context.Context, v int) (int, error) {
		if v == 2 {
			return 0, errors.New("boom: two is forbidden")
		}
		return v * 10, nil
	}))
	return duro.Run(ctx, ns, duro.Pipe4(
		duro.Expand("explode", explode),
		duro.Rescue("rescue", boom, func(_ context.Context, in int, _ error) (int, error) {
			return -in, nil
		}),
		duro.Step("post", func(_ context.Context, v int) (int, error) {
			rescuePostRuns.Add(1)
			return v, nil
		}),
		duro.Collect[int]("collect"),
	))
}

// rescuePartialWorkflow's embedded pipeline expands each item into two and
// fails on value 3: item 2's successful first emission must be discarded in
// favor of the fallback, while item 5's two emissions pass through in order.
func rescuePartialWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	inner := duro.Pipe2(
		duro.Expand("fan", func(_ context.Context, v int) ([]int, error) {
			return []int{v, v + 1}, nil
		}),
		duro.Step("check", func(_ context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errors.New("three is forbidden")
			}
			return v, nil
		}),
	)
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Rescue("rescue", inner, func(_ context.Context, in int, _ error) (int, error) {
			return -in, nil
		}),
		duro.Collect[int]("collect"),
	))
}

// rescueRethrowWorkflow's handler transforms the failure and returns it: the
// outer pipeline must fail with the transformed error and stay fail-fast for
// the items behind the failure.
func rescueRethrowWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	inner := duro.Pipe1(duro.Step("check", func(_ context.Context, v int) (int, error) {
		if v == 2 {
			return 0, errors.New("two is forbidden")
		}
		rescueRethrowSurvivorRuns.Add(1)
		return v, nil
	}))
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Rescue("rescue", inner, func(_ context.Context, _ int, cause error) (int, error) {
			return 0, fmt.Errorf("reported then rethrown: %w", cause)
		}),
		duro.Collect[int]("collect"),
	))
}

// rescueUpstreamWorkflow fails before the Rescue stage: only embedded
// failures are rescuable, so the handler must never run.
func rescueUpstreamWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe2(
		duro.Step("pre", func(_ context.Context, _ int) (int, error) {
			return 0, errors.New("upstream failure")
		}),
		duro.Rescue("rescue", duro.Pipe1(duro.Pure("id", func(v int) int { return v })),
			func(_ context.Context, in int, _ error) (int, error) {
				rescueUpstreamHandlerRuns.Add(1)
				return in, nil
			}),
	))
}

// rescueRetryWorkflow is the retry-then-swallow shape: real stage retry
// options on the embedded step, with the handler firing only after they are
// exhausted.
func rescueRetryWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	flaky := duro.Pipe1(duro.Step("always-fails", func(_ context.Context, _ int) (int, error) {
		rescueRetryAttempts.Add(1)
		return 0, errors.New("permanently down")
	}, duro.WithMaxRetries(2), duro.WithBaseInterval(time.Millisecond)))
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Rescue("best-effort", flaky, func(_ context.Context, in int, _ error) (int, error) {
			return in, nil
		}),
	))
}

// rescueNestedWorkflow nests Rescue in Rescue: the inner handler wins for
// inner failures, and rethrowing from it escalates to the outer handler.
func rescueNestedWorkflow(ctx dbos.DBOSContext, n int) (string, error) {
	boom := duro.Pipe1(duro.Step("boom", func(_ context.Context, v int) (string, error) {
		return "", fmt.Errorf("boom on %d", v)
	}))
	inner := duro.Pipe1(duro.Rescue("inner", boom, func(_ context.Context, v int, cause error) (string, error) {
		if v == 2 {
			return "", fmt.Errorf("escalated: %w", cause)
		}
		return "inner-rescued", nil
	}))
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Rescue("outer", inner, func(_ context.Context, _ int, cause error) (string, error) {
			return "outer-rescued: " + cause.Error(), nil
		}),
	))
}

// errCrashSentinel is a typed sentinel returned by the crash fixture's
// failing step. The handler records whether errors.Is still matches it,
// pinning the replay-stable cause contract: it must NOT match, on the live
// path or on recovery — identity would otherwise diverge between the two, so
// Rescue strips the cause to its message on both.
var errCrashSentinel = errors.New("boom: crash-window failure")

// rescueCrashWorkflow is the crash-recovery fixture: pre (step 1), a rescued
// failing step (step 2), the handler checkpoint (step 3), post (step 4).
// Forking between steps 2 and 3 simulates a process death after the embedded
// failure was recorded but before the rescue decision was.
func rescueCrashWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	boom := duro.Pipe1(duro.Step("boom", func(_ context.Context, _ int) (int, error) {
		rescueCrashBoomRuns.Add(1)
		return 0, errCrashSentinel
	}))
	return duro.Run(ctx, n, duro.Pipe3(
		duro.Step("pre", func(_ context.Context, v int) (int, error) { return v + 1, nil }),
		duro.Rescue("rescue", boom, func(_ context.Context, in int, cause error) (int, error) {
			rescueCrashHandlerRuns.Add(1)
			rescueCrashObservedCause.Store(cause.Error())
			rescueCrashSawSentinel.Store(errors.Is(cause, errCrashSentinel))
			return -in, nil
		}),
		duro.Step("post", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
	))
}

// fanChildSquareOrFail is the child workflow for the Rescue+FanOut test:
// squares non-negatives, fails on negatives.
func fanChildSquareOrFail(_ dbos.DBOSContext, n int) (int, error) {
	if n < 0 {
		return 0, fmt.Errorf("fan child: negative input %d", n)
	}
	return n * n, nil
}

// rescueFanQueue is only referenced inside a Rescue-embedded pipeline;
// registering the pipeline must still auto-register it.
var rescueFanQueue = duro.NewQueue("duro-test-rescue-fan", duro.WithConcurrency(2))

var rescueFanWf *duro.PipelineWorkflow[[]int, []int]

// rescueParallelWorkflow runs a Parallel fleet inside a rescued segment: each
// item spreads into three concurrent squares, and a fleet containing a
// negative fails as a unit — rescued to the original item, with the next
// item's fleet unaffected.
func rescueParallelWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	fleet := duro.Pipe2(
		duro.Expand("spread", func(_ context.Context, v int) ([]int, error) {
			return []int{v, v + 1, v + 2}, nil
		}),
		duro.Parallel("squares", 2, func(_ context.Context, v int) (int, error) {
			if v < 0 {
				return 0, fmt.Errorf("cannot square a debt of %d", v)
			}
			return v * v, nil
		}),
	)
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Rescue("fleet-guard", fleet, func(_ context.Context, in int, _ error) (int, error) {
			return in, nil
		}),
		duro.Collect[int]("collect"),
	))
}

var (
	viaEffectRuns      atomic.Int64
	viaAfterStageRuns  atomic.Int64
	viaFanChildRuns    atomic.Int64
	viaRescuedPostRuns atomic.Int64
)

// viaPassThroughWorkflow routes each item through a Via whose embedded
// pipeline emits zero, one, or many values depending on the item — the
// original item must come out the other side in every case.
func viaPassThroughWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	effects := duro.Pipe2(
		// 0 → drop (zero emissions), 1 → [v] (one), 2 → [v, v] (many).
		duro.Expand("fan", func(_ context.Context, v int) ([]int, error) {
			outs := make([]int, v%3)
			for i := range outs {
				outs[i] = v * 100 // values that must NOT appear downstream
			}
			return outs, nil
		}),
		duro.Tap("effect", func(_ context.Context, _ int) error {
			viaEffectRuns.Add(1)
			return nil
		}),
	)
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Via("side-quest", effects),
		duro.Collect[int]("collect"),
	))
}

// viaFailureWorkflow fails inside the Via's embedded pipeline on item 2:
// the outer pipeline must fail fast and the stage after the Via must never
// run for items behind the failure.
func viaFailureWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	boom := duro.Pipe1(duro.Tap("boom", func(_ context.Context, v int) error {
		if v == 2 {
			return errors.New("boom: two is forbidden")
		}
		return nil
	}))
	return duro.Run(ctx, ns, duro.Pipe4(
		duro.Expand("explode", explode),
		duro.Via("side-quest", boom),
		duro.Step("after", func(_ context.Context, v int) (int, error) {
			viaAfterStageRuns.Add(1)
			return v, nil
		}),
		duro.Collect[int]("collect"),
	))
}

// viaRescuedWorkflow composes the best-effort fan-out shape:
// Rescue(Pipe1(Via(...))) continues with the original item whether the
// embedded effects succeeded or failed.
func viaRescuedWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	boom := duro.Pipe1(duro.Tap("boom", func(_ context.Context, v int) error {
		if v == 2 {
			return errors.New("boom: two is forbidden")
		}
		return nil
	}))
	return duro.Run(ctx, ns, duro.Pipe4(
		duro.Expand("explode", explode),
		duro.Rescue("best-effort", duro.Pipe1(duro.Via("side-quest", boom)),
			func(_ context.Context, in int, _ error) (int, error) {
				return in, nil // the original item continues either way
			}),
		duro.Step("post", func(_ context.Context, v int) (int, error) {
			viaRescuedPostRuns.Add(1)
			return v, nil
		}),
		duro.Collect[int]("collect"),
	))
}

// viaFanChildLog is the child workflow for the Via+FanOut test; the parent
// discards its results, so it only counts executions.
func viaFanChildLog(_ dbos.DBOSContext, n int) (int, error) {
	viaFanChildRuns.Add(1)
	return n, nil
}

// viaFanQueue is only referenced inside a Via-embedded pipeline; registering
// the pipeline must still auto-register it.
var viaFanQueue = duro.NewQueue("duro-test-via-fan", duro.WithConcurrency(2))

var viaFanWf *duro.PipelineWorkflow[[]int, []int]

// includeExtraViaStage simulates editing a Via's embedded pipeline between
// the original run and a replay, for the shape-guard test.
var includeExtraViaStage = false

func mutableViaWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	effects := duro.Pipe1(duro.Tap("noop", func(_ context.Context, _ int) error { return nil }))
	if includeExtraViaStage {
		effects = duro.Pipe2(
			duro.Tap("noop", func(_ context.Context, _ int) error { return nil }),
			duro.Tap("extra", func(_ context.Context, _ int) error { return nil }),
		)
	}
	return duro.Run(ctx, n, duro.Pipe2(
		duro.Via("side-quest", effects),
		duro.Step("double", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
	))
}

// flowQueue is only referenced inside a Branch arm; registering the pipeline
// must still auto-register it.
var flowQueue = duro.NewQueue("duro-test-flow", duro.WithConcurrency(2))

var flowQueueWf *duro.PipelineWorkflow[[]int, []int]

func registerFlowWorkflows(ctx dbos.DBOSContext) {
	dbos.RegisterWorkflow(ctx, branchWorkflow, dbos.WithWorkflowName("branchWorkflow"))
	dbos.RegisterWorkflow(ctx, switchWorkflow, dbos.WithWorkflowName("switchWorkflow"))
	dbos.RegisterWorkflow(ctx, loopWorkflow, dbos.WithWorkflowName("loopWorkflow"))
	dbos.RegisterWorkflow(ctx, subWorkflow, dbos.WithWorkflowName("subWorkflow"))
	dbos.RegisterWorkflow(ctx, collectEmptyWorkflow, dbos.WithWorkflowName("collectEmptyWorkflow"))
	dbos.RegisterWorkflow(ctx, mutableBranchWorkflow, dbos.WithWorkflowName("mutableBranchWorkflow"))
	dbos.RegisterWorkflow(ctx, loopDropWorkflow, dbos.WithWorkflowName("loopDropWorkflow"))
	dbos.RegisterWorkflow(ctx, branchFailureWorkflow, dbos.WithWorkflowName("branchFailureWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueSwallowWorkflow, dbos.WithWorkflowName("rescueSwallowWorkflow"))
	dbos.RegisterWorkflow(ctx, rescuePartialWorkflow, dbos.WithWorkflowName("rescuePartialWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueRethrowWorkflow, dbos.WithWorkflowName("rescueRethrowWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueUpstreamWorkflow, dbos.WithWorkflowName("rescueUpstreamWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueRetryWorkflow, dbos.WithWorkflowName("rescueRetryWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueNestedWorkflow, dbos.WithWorkflowName("rescueNestedWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueCrashWorkflow, dbos.WithWorkflowName("rescueCrashWorkflow"))
	dbos.RegisterWorkflow(ctx, rescueParallelWorkflow, dbos.WithWorkflowName("rescueParallelWorkflow"))
	dbos.RegisterWorkflow(ctx, fanChildSquareOrFail, dbos.WithWorkflowName("fanChildSquareOrFail"))
	dbos.RegisterWorkflow(ctx, viaPassThroughWorkflow, dbos.WithWorkflowName("viaPassThroughWorkflow"))
	dbos.RegisterWorkflow(ctx, viaFailureWorkflow, dbos.WithWorkflowName("viaFailureWorkflow"))
	dbos.RegisterWorkflow(ctx, viaRescuedWorkflow, dbos.WithWorkflowName("viaRescuedWorkflow"))
	dbos.RegisterWorkflow(ctx, mutableViaWorkflow, dbos.WithWorkflowName("mutableViaWorkflow"))
	dbos.RegisterWorkflow(ctx, viaFanChildLog, dbos.WithWorkflowName("viaFanChildLog"))

	flowQueueWf = duro.Register(app, "flowQueuePipeline", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Branch("maybe-fan", func(_ context.Context, v int) (bool, error) { return v > 0, nil },
			duro.Pipe1(duro.FanOut("fan", flowQueue, duro.Workflow(fanChildSquare))),
			duro.Pipe1(duro.Pure("as-is", func(v int) int { return v })),
		),
		duro.Collect[int]("collect"),
	))

	rescueFanWf = duro.Register(app, "rescueFanPipeline", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Rescue("fan-guard", duro.Pipe1(duro.FanOut("fan", rescueFanQueue, duro.Workflow(fanChildSquareOrFail))),
			func(_ context.Context, in int, _ error) (int, error) {
				return in, nil // pass-through swallow: keep the failed child's item as-is
			}),
		duro.Collect[int]("collect"),
	))

	viaFanWf = duro.Register(app, "viaFanPipeline", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Via("notify-all", duro.Pipe1(duro.FanOut("fan", viaFanQueue, duro.Workflow(viaFanChildLog)))),
		duro.Collect[int]("collect"),
	))
}

// --- tests ------------------------------------------------------------------

// TestBranchRoutesPerItem proves Branch checkpoints the routing decision and
// runs exactly one arm per item, with the step sequence to show it.
func TestBranchRoutesPerItem(t *testing.T) {
	halveRuns.Store(0)
	tripleRuns.Store(0)

	result, wfID := mustRun(t, branchWorkflow, []int{1, 2})
	assertInts(t, result, []int{3, 1}) // 1*3, 2/2
	if h, tr := halveRuns.Load(), tripleRuns.Load(); h != 1 || tr != 1 {
		t.Errorf("halve/triple executions = %d/%d, want 1/1", h, tr)
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName, "explode",
		"route", "triple", "collect", // item 1 (odd)
		"route", "halve", "collect", // item 2 (even)
	})
}

// TestBranchDurabilityRerun proves a completed branched run replays without
// re-executing either arm.
func TestBranchDurabilityRerun(t *testing.T) {
	halveRuns.Store(0)
	tripleRuns.Store(0)
	const wfID = "branch-durability-rerun"

	first, _ := mustRun(t, branchWorkflow, []int{5, 6, 7}, dbos.WithWorkflowID(wfID))
	h, tr := halveRuns.Load(), tripleRuns.Load()

	second, _ := mustRun(t, branchWorkflow, []int{5, 6, 7}, dbos.WithWorkflowID(wfID))
	assertInts(t, second, first)
	if halveRuns.Load() != h || tripleRuns.Load() != tr {
		t.Errorf("arm executions changed on rerun: %d/%d → %d/%d", h, tr, halveRuns.Load(), tripleRuns.Load())
	}
}

// TestSwitchDispatch proves multi-way dispatch and the unmatched-key failure.
func TestSwitchDispatch(t *testing.T) {
	result, _ := mustRun(t, switchWorkflow, []string{"upper", "tag"})
	if len(result) != 2 || result[0] != "UPPER" || result[1] != "#tag" {
		t.Errorf("result = %v, want [UPPER #tag]", result)
	}

	handle, err := dbos.RunWorkflow(dctx, switchWorkflow, []string{"boom"})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), `no case for route key "boom"`) {
		t.Fatalf("workflow error = %v, want the unmatched-key failure", err)
	}
}

// TestLoopIteratesUntilDone proves the durable do-while: recorded predicate
// verdicts reproduce the iteration count, and a rerun replays for free.
func TestLoopIteratesUntilDone(t *testing.T) {
	loopRuns.Store(0)
	const wfID = "loop-durability-rerun"

	result, _ := mustRun(t, loopWorkflow, 1, dbos.WithWorkflowID(wfID))
	if result != 4 {
		t.Errorf("result = %d, want 4", result)
	}
	if got := loopRuns.Load(); got != 3 {
		t.Errorf("body executions = %d, want 3 (1→2→3→4)", got)
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName,
		"inc", "until-four", "inc", "until-four", "inc", "until-four",
	})

	second, _ := mustRun(t, loopWorkflow, 1, dbos.WithWorkflowID(wfID))
	if second != 4 || loopRuns.Load() != 3 {
		t.Errorf("rerun result/executions = %d/%d, want 4/3 (replayed, not re-run)", second, loopRuns.Load())
	}
}

// TestSubIsWholeStream proves Sub embeds a pipeline over the whole stream:
// the embedded Reduce folds every item.
func TestSubIsWholeStream(t *testing.T) {
	result, _ := mustRun(t, subWorkflow, []int{1, 2, 3, 4})
	if result != 10 {
		t.Errorf("result = %d, want 10", result)
	}
}

// TestCollectEmptyStream proves Collect yields an empty slice — not
// ErrNoValue — when everything was filtered out.
func TestCollectEmptyStream(t *testing.T) {
	result, _ := mustRun(t, collectEmptyWorkflow, 42)
	if len(result) != 0 {
		t.Errorf("result = %v, want empty", result)
	}
}

// TestNestedShapeGuard proves editing a Branch arm between run and replay
// trips the shape-mismatch guard: nested pipelines are part of the shape.
func TestNestedShapeGuard(t *testing.T) {
	includeExtraArmStage = false
	result, wfID := mustRun(t, mutableBranchWorkflow, 21)
	if result != 42 {
		t.Fatalf("result = %d, want 42", result)
	}

	includeExtraArmStage = true
	defer func() { includeExtraArmStage = false }()

	handle, err := duro.ForkFromStage[int](app, duro.Fork{WorkflowID: wfID, Stage: "route"})
	if err != nil {
		t.Fatalf("forking: %v", err)
	}
	_, err = handle.Result()
	if err == nil || !strings.Contains(err.Error(), "pipeline shape mismatch") {
		t.Fatalf("forked workflow error = %v, want a pipeline shape mismatch", err)
	}
}

// TestFlowQueuePropagation proves queues referenced only inside an embedded
// pipeline are still auto-registered by Register.
func TestFlowQueuePropagation(t *testing.T) {
	handle, err := flowQueueWf.Start(app, []int{2, -3, 4})
	if err != nil {
		t.Fatalf("starting pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	assertInts(t, result, []int{4, -3, 16}) // positives squared on the queue, negative passed through
}

// TestFlowConstructionPanics covers the control-flow validation layer.
func TestFlowConstructionPanics(t *testing.T) {
	valid := duro.Pipe1(duro.Pure("id", func(v int) int { return v }))
	pred := func(_ context.Context, _ int) (bool, error) { return true, nil }

	assertPanics(t, "Branch zero-value arm", func() {
		duro.Branch("b", pred, valid, duro.Pipeline[int, int]{})
	})
	assertPanics(t, "Branch nil predicate", func() {
		duro.Branch("b", nil, valid, valid)
	})
	assertPanics(t, "Switch without cases", func() {
		duro.Switch[int, int]("s", func(_ context.Context, _ int) (string, error) { return "", nil })
	})
	assertPanics(t, "Switch duplicate case", func() {
		duro.Switch("s", func(_ context.Context, _ int) (string, error) { return "a", nil },
			duro.When("a", valid), duro.When("a", valid))
	})
	assertPanics(t, "When empty key", func() {
		duro.When("", valid)
	})
	assertPanics(t, "Loop nil predicate", func() {
		duro.Loop("l", valid, nil)
	})
	assertPanics(t, "Sub zero-value pipeline", func() {
		duro.Sub("s", duro.Pipeline[int, int]{})
	})
	assertPanics(t, "Rescue nil handler", func() {
		duro.Rescue("r", valid, nil)
	})
	assertPanics(t, "Rescue zero-value pipeline", func() {
		duro.Rescue("r", duro.Pipeline[int, int]{}, func(_ context.Context, v int, _ error) (int, error) {
			return v, nil
		})
	})
	assertPanics(t, "Via empty name", func() {
		duro.Via("", valid)
	})
	assertPanics(t, "Via zero-value pipeline", func() {
		duro.Via("v", duro.Pipeline[int, int]{})
	})
}

// TestLoopBodyDropsItem proves a body that emits nothing (a Filter inside)
// drops the item like Filter does, ending the loop without a value.
func TestLoopBodyDropsItem(t *testing.T) {
	handle, err := dbos.RunWorkflow(dctx, loopDropWorkflow, 1)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "without emitting a value") {
		t.Fatalf("workflow error = %v, want ErrNoValue (the loop dropped its item)", err)
	}
}

// TestBranchArmFailureAborts proves a stage failing inside an embedded arm
// fails the pipeline and stops items queued behind it — the abort machinery
// reaches into embedded pipelines.
func TestBranchArmFailureAborts(t *testing.T) {
	armAfterFailureRuns.Store(0)

	handle, err := dbos.RunWorkflow(dctx, branchFailureWorkflow, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "two is forbidden") {
		t.Fatalf("workflow error = %v, want the arm failure", err)
	}
	// Item 1 passed, item 2 failed inside the arm, item 3 must never route.
	if got := armAfterFailureRuns.Load(); got != 1 {
		t.Errorf("arm executions = %d, want 1 (only item 1)", got)
	}
}

// TestRescueSwallowContinues proves the abort from a rescued failure is
// contained: stages after the Rescue keep executing, both for the rescued
// item and for the items behind it, and the handler checkpoint appears only
// where the embedded run failed.
func TestRescueSwallowContinues(t *testing.T) {
	rescuePostRuns.Store(0)

	result, wfID := mustRun(t, rescueSwallowWorkflow, []int{1, 2, 3})
	assertInts(t, result, []int{10, -2, 30})
	if got := rescuePostRuns.Load(); got != 3 {
		t.Errorf("post executions = %d, want 3 (the abort leaked past the Rescue)", got)
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName, "explode",
		"boom", "post", "collect", // item 1
		"boom", "rescue", "post", "collect", // item 2 (failed, rescued)
		"boom", "post", "collect", // item 3
	})
}

// TestRescueDiscardsPartialEmissions proves a failed embedded run is a unit:
// its successful emissions before the failure are discarded and only the
// fallback is emitted, while a successful run's emissions pass through in
// order.
func TestRescueDiscardsPartialEmissions(t *testing.T) {
	result, _ := mustRun(t, rescuePartialWorkflow, []int{2, 5})
	// Item 2 fans to [2, 3]: check(2) succeeds, check(3) fails — the partial
	// emission is dropped for the fallback. Item 5 fans to [5, 6]: both pass.
	assertInts(t, result, []int{-2, 5, 6})
}

// TestRescueRethrowFailsPipeline proves a handler that returns an error fails
// the outer pipeline with it, fail-fast for the items behind the failure.
func TestRescueRethrowFailsPipeline(t *testing.T) {
	rescueRethrowSurvivorRuns.Store(0)

	handle, err := dbos.RunWorkflow(dctx, rescueRethrowWorkflow, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "reported then rethrown: two is forbidden") {
		t.Fatalf("workflow error = %v, want the transformed failure", err)
	}
	// Item 1 passed, item 2's failure was rethrown, item 3 must never run.
	if got := rescueRethrowSurvivorRuns.Load(); got != 1 {
		t.Errorf("embedded executions = %d, want 1 (only item 1)", got)
	}
}

// TestRescueUpstreamFailureBypassesHandler proves Rescue intercepts only its
// embedded pipeline's failures: an upstream error propagates untouched.
func TestRescueUpstreamFailureBypassesHandler(t *testing.T) {
	rescueUpstreamHandlerRuns.Store(0)

	handle, err := dbos.RunWorkflow(dctx, rescueUpstreamWorkflow, 1)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "upstream failure") {
		t.Fatalf("workflow error = %v, want the upstream failure", err)
	}
	if got := rescueUpstreamHandlerRuns.Load(); got != 0 {
		t.Errorf("handler executions = %d, want 0 (upstream failures are not rescuable)", got)
	}
}

// TestRescueRetryThenSwallow proves stage retry options compose with
// swallowing: the handler fires only after the embedded step's retries are
// exhausted.
func TestRescueRetryThenSwallow(t *testing.T) {
	rescueRetryAttempts.Store(0)

	result, _ := mustRun(t, rescueRetryWorkflow, 7)
	if result != 7 {
		t.Errorf("result = %d, want the pass-through fallback 7", result)
	}
	if got := rescueRetryAttempts.Load(); got != 3 {
		t.Errorf("step attempts = %d, want 3 (1 + WithMaxRetries(2))", got)
	}
}

// TestRescueNested proves the innermost enclosing Rescue wins, and that
// rethrowing from an inner handler escalates to the outer one.
func TestRescueNested(t *testing.T) {
	swallowed, _ := mustRun(t, rescueNestedWorkflow, 1)
	if swallowed != "inner-rescued" {
		t.Errorf("result = %q, want %q (outer handler must not fire)", swallowed, "inner-rescued")
	}

	escalated, _ := mustRun(t, rescueNestedWorkflow, 2)
	if want := "outer-rescued: escalated: boom on 2"; escalated != want {
		t.Errorf("result = %q, want %q", escalated, want)
	}
}

// TestRescueRecoveryMidCrash simulates the two crash windows around the
// handler checkpoint. Forking before the handler's step slot (a death between
// the embedded failure and the rescue decision) must replay the recorded
// failure — not re-execute the failed step — and re-reach the handler with a
// cause byte-identical to the live one: the handler cannot behave differently
// on recovery than it did live. Forking after the slot must replay the
// recorded decision without running the handler again: recovery never flips
// a swallowed failure into a propagated one.
func TestRescueRecoveryMidCrash(t *testing.T) {
	rescueCrashBoomRuns.Store(0)
	rescueCrashHandlerRuns.Store(0)

	// Steps: 0 duro.shape, 1 pre, 2 boom (fails), 3 rescue (handler), 4 post.
	original, originalID := mustRun(t, rescueCrashWorkflow, 1)
	if original != -4 { // pre: 2, rescued to -2, post: -4
		t.Fatalf("result = %d, want -4", original)
	}
	if b, h := rescueCrashBoomRuns.Load(), rescueCrashHandlerRuns.Load(); b != 1 || h != 1 {
		t.Fatalf("boom/handler executions = %d/%d, want 1/1", b, h)
	}
	liveCause, _ := rescueCrashObservedCause.Load().(string)
	if liveCause != errCrashSentinel.Error() {
		t.Fatalf("live cause = %q, want exactly %q", liveCause, errCrashSentinel.Error())
	}
	if rescueCrashSawSentinel.Load() {
		t.Fatal("errors.Is matched the sentinel on the live path — the cause must be stripped to its message so live and recovered handlers see the same value")
	}

	// Crash window 1: died before the handler checkpoint.
	handle, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: originalID,
		StartStep:          3,
	})
	if err != nil {
		t.Fatalf("forking before the handler: %v", err)
	}
	recovered, err := handle.GetResult()
	if err != nil {
		t.Fatalf("recovered workflow failed: %v", err)
	}
	if recovered != original {
		t.Errorf("recovered result = %d, want %d", recovered, original)
	}
	if got := rescueCrashBoomRuns.Load(); got != 1 {
		t.Errorf("boom executions = %d, want 1 (the recorded failure must replay, not re-execute)", got)
	}
	if got := rescueCrashHandlerRuns.Load(); got != 2 {
		t.Errorf("handler executions = %d, want 2 (recovery must re-reach the handler)", got)
	}
	if recoveredCause, _ := rescueCrashObservedCause.Load().(string); recoveredCause != liveCause {
		t.Errorf("recovered cause = %q, want the live cause %q byte for byte", recoveredCause, liveCause)
	}
	if rescueCrashSawSentinel.Load() {
		t.Error("errors.Is matched the sentinel on recovery — identity must not differ between the live path and recovery")
	}

	// Crash window 2: died after the handler checkpoint — the rescue decision
	// itself must replay.
	handle, err = dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: originalID,
		StartStep:          4,
	})
	if err != nil {
		t.Fatalf("forking after the handler: %v", err)
	}
	replayed, err := handle.GetResult()
	if err != nil {
		t.Fatalf("replayed workflow failed: %v", err)
	}
	if replayed != original {
		t.Errorf("replayed result = %d, want %d", replayed, original)
	}
	if b, h := rescueCrashBoomRuns.Load(), rescueCrashHandlerRuns.Load(); b != 1 || h != 2 {
		t.Errorf("boom/handler executions = %d/%d, want 1/2 (the recorded decision must replay)", b, h)
	}
}

// TestRescueEmbeddedFanOut proves containment reaches the heaviest embedded
// stage: a FanOut child failing on the queue is rescued like any stage error,
// the items around it still fan out, and the queue referenced only inside the
// rescued pipeline is still auto-registered by Register.
func TestRescueEmbeddedFanOut(t *testing.T) {
	handle, err := rescueFanWf.Start(app, []int{2, -3, 4})
	if err != nil {
		t.Fatalf("starting pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	assertInts(t, result, []int{4, -3, 16}) // children square; the failed child's item passes through rescued
}

// TestRescueEmbeddedParallel proves a Parallel fleet inside a rescued segment
// fails as a unit and is contained: the fleet's partial results are discarded
// for the fallback, its siblings are drained before the outer pipeline
// continues, and the next item's fleet still runs.
func TestRescueEmbeddedParallel(t *testing.T) {
	result, _ := mustRun(t, rescueParallelWorkflow, []int{1, -2, 3})
	// 1 spreads to [1,2,3] and squares; -2's fleet contains negatives and is
	// rescued to the item itself; 3 spreads to [3,4,5] and squares.
	assertInts(t, result, []int{1, 4, 9, -2, 9, 16, 25})
}

// TestViaPassesItemThrough proves Via emits the original item whether the
// embedded pipeline emitted zero, one, or many values — and that Via records
// no step of its own: the recorded sequence is exactly the embedded stages'.
func TestViaPassesItemThrough(t *testing.T) {
	viaEffectRuns.Store(0)

	// v%3 embedded emissions per item: 3 → zero, 4 → one, 5 → two.
	result, wfID := mustRun(t, viaPassThroughWorkflow, []int{3, 4, 5})
	assertInts(t, result, []int{3, 4, 5}) // the originals — never the embedded 100s
	if got := viaEffectRuns.Load(); got != 3 {
		t.Errorf("embedded effect executions = %d, want 3 (0+1+2)", got)
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName, "explode",
		"fan", "collect", // item 3: embedded pipeline emitted nothing
		"fan", "effect", "collect", // item 4
		"fan", "effect", "effect", "collect", // item 5
	})
}

// TestViaFailurePropagates proves an embedded failure is fail-fast and
// unscoped: the outer pipeline fails with it and stages behind the failure
// never run.
func TestViaFailurePropagates(t *testing.T) {
	viaAfterStageRuns.Store(0)

	handle, err := dbos.RunWorkflow(dctx, viaFailureWorkflow, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "two is forbidden") {
		t.Fatalf("workflow error = %v, want the embedded failure", err)
	}
	// Item 1 passed the Via, item 2 failed inside it, item 3 must never run.
	if got := viaAfterStageRuns.Load(); got != 1 {
		t.Errorf("after-stage executions = %d, want 1 (only item 1)", got)
	}
}

// TestViaRescuedContinues proves the best-effort fan-out composition:
// Rescue(Pipe1(Via(...))) continues with the original item whether the
// embedded effects succeeded or failed.
func TestViaRescuedContinues(t *testing.T) {
	viaRescuedPostRuns.Store(0)

	result, _ := mustRun(t, viaRescuedWorkflow, []int{1, 2, 3})
	assertInts(t, result, []int{1, 2, 3})
	if got := viaRescuedPostRuns.Load(); got != 3 {
		t.Errorf("post executions = %d, want 3 (the rescued item and the items behind it)", got)
	}
}

// TestViaEmbeddedFanOut proves the flagship use: children fan out on a queue
// referenced only inside the Via (still auto-registered), complete before the
// original items are emitted, and a rerun of the same workflow ID re-attaches
// to them instead of re-running.
func TestViaEmbeddedFanOut(t *testing.T) {
	viaFanChildRuns.Store(0)
	const wfID = "via-fanout-rerun"

	handle, err := viaFanWf.Start(app, []int{7, 8}, duro.WithWorkflowID(wfID))
	if err != nil {
		t.Fatalf("starting pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	assertInts(t, result, []int{7, 8}) // the originals, not the children's results
	if got := viaFanChildRuns.Load(); got != 2 {
		t.Fatalf("child executions = %d, want 2", got)
	}

	rerun, err := viaFanWf.Start(app, []int{7, 8}, duro.WithWorkflowID(wfID))
	if err != nil {
		t.Fatalf("re-starting pipeline: %v", err)
	}
	replayed, err := rerun.Result()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	assertInts(t, replayed, result)
	if got := viaFanChildRuns.Load(); got != 2 {
		t.Errorf("child executions after rerun = %d, want 2 (re-attached, not re-run)", got)
	}
}

// TestViaShapeGuard proves the embedded pipeline is part of the shape:
// editing it between run and replay trips the shape-mismatch guard.
func TestViaShapeGuard(t *testing.T) {
	includeExtraViaStage = false
	result, wfID := mustRun(t, mutableViaWorkflow, 21)
	if result != 42 {
		t.Fatalf("result = %d, want 42", result)
	}

	includeExtraViaStage = true
	defer func() { includeExtraViaStage = false }()

	handle, err := duro.ForkFromStage[int](app, duro.Fork{WorkflowID: wfID, Stage: "double"})
	if err != nil {
		t.Fatalf("forking: %v", err)
	}
	_, err = handle.Result()
	if err == nil || !strings.Contains(err.Error(), "pipeline shape mismatch") {
		t.Fatalf("forked workflow error = %v, want a pipeline shape mismatch", err)
	}
}
