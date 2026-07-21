package duro_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

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

	flowQueueWf = duro.Register(app, "flowQueuePipeline", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Branch("maybe-fan", func(_ context.Context, v int) (bool, error) { return v > 0, nil },
			duro.Pipe1(duro.FanOut("fan", flowQueue, duro.Workflow(fanChildSquare))),
			duro.Pipe1(duro.Pure("as-is", func(v int) int { return v })),
		),
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
