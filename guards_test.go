package duro_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// Guards against the foot guns that previously escaped every safety layer:
// silently discarded pipeline results, ambiguous stage names, an accidental
// zero meaning "unbounded", an unbounded durable Loop, and a workflow name
// crossing a process boundary as an unchecked string.

// assertPanicsWith runs fn and requires it to panic with a message containing
// want. Matching the message keeps a test from passing on an unrelated panic.
func assertPanicsWith(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected a panic mentioning %q, got none", want)
			return
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, want) {
			t.Errorf("panic = %v, want it to mention %q", r, want)
		}
	}()
	fn()
}

func guardStep(name string) duro.Stage[int, int] {
	return duro.Step(name, func(_ context.Context, v int) (int, error) { return v, nil })
}

// --- Stage-name uniqueness -------------------------------------------------

// TestDuplicateStageNamesRejected proves two stages cannot share a name inside
// one pipeline. A name is how a stage is identified in the recorded step list,
// so duplicates make ForkFromStage silently target whichever ran first.
func TestDuplicateStageNamesRejected(t *testing.T) {
	assertPanicsWith(t, `two stages named "process"`, func() {
		duro.Pipe2(guardStep("process"), guardStep("process"))
	})
	// Non-adjacent, and across different stage kinds.
	assertPanicsWith(t, `two stages named "audit"`, func() {
		duro.Pipe3(
			duro.Tap("audit", func(_ context.Context, _ int) error { return nil }),
			guardStep("middle"),
			guardStep("audit"),
		)
	})
}

// TestDuplicateStageNamesAcrossBranchArmsAllowed proves the check does not
// over-reach: only one arm of a Branch or Switch ever runs, so arms may reuse
// names without making anything ambiguous.
func TestDuplicateStageNamesAcrossBranchArmsAllowed(t *testing.T) {
	pred := func(_ context.Context, _ int) (bool, error) { return true, nil }
	duro.Pipe1(duro.Branch("route", pred,
		duro.Pipe1(guardStep("notify")),
		duro.Pipe1(guardStep("notify")),
	))
	duro.Pipe1(duro.Switch("dispatch",
		func(_ context.Context, _ int) (string, error) { return "a", nil },
		duro.When("a", duro.Pipe1(guardStep("handle"))),
		duro.When("b", duro.Pipe1(guardStep("handle"))),
	))
}

// TestReservedStageNamePrefixRejected proves user stages cannot claim duro's
// own bookkeeping namespace, which would make them indistinguishable from
// internal steps in the recorded step list ForkFromStage reads.
func TestReservedStageNamePrefixRejected(t *testing.T) {
	assertPanicsWith(t, "reserved", func() { guardStep(duro.ShapeStepName) })
	assertPanicsWith(t, "reserved", func() { guardStep("duro.anything") })
	assertPanicsWith(t, "reserved", func() { duro.Delay[int]("duro.wait", time.Second) })
	assertPanicsWith(t, "reserved", func() { duro.Sub("duro.sub", duro.Pipe1(guardStep("x"))) })
	// A name that merely contains the prefix elsewhere is fine.
	guardStep("my-duro.step")
}

// --- Parallel concurrency --------------------------------------------------

// TestParallelRequiresExplicitConcurrency proves a zero concurrency limit is
// rejected rather than silently meaning "unlimited" — the reading an unset
// config field or an empty slice's length would otherwise get.
func TestParallelRequiresExplicitConcurrency(t *testing.T) {
	echo := func(_ context.Context, v int) (int, error) { return v, nil }
	assertPanicsWith(t, "positive concurrency limit or duro.Unbounded", func() {
		duro.Parallel("par", 0, echo)
	})
	assertPanicsWith(t, "positive concurrency limit or duro.Unbounded", func() {
		duro.Parallel("par", -2, echo)
	})
	// Both legitimate forms construct.
	duro.Parallel("bounded", 4, echo)
	duro.Parallel("unbounded", duro.Unbounded, echo)
}

// --- Loop bound ------------------------------------------------------------

// TestMaxIterationsRejectedOutsideLoop proves the option fails at construction
// on every stage that cannot honor it, rather than being silently ignored.
func TestMaxIterationsRejectedOutsideLoop(t *testing.T) {
	opt := duro.WithMaxIterations(3)
	echo := func(_ context.Context, v int) (int, error) { return v, nil }
	assertPanicsWith(t, "applies only to Loop", func() { duro.Step("s", echo, opt) })
	assertPanicsWith(t, "applies only to Loop", func() { duro.Parallel("p", 2, echo, opt) })
	assertPanicsWith(t, "applies only to Loop", func() {
		duro.Filter("f", func(_ context.Context, _ int) (bool, error) { return true, nil }, opt)
	})
	assertPanicsWith(t, "applies only to Loop", func() {
		duro.Rescue("r", duro.Pipe1(guardStep("x")),
			func(_ context.Context, v int, _ error) (int, error) { return v, nil }, opt)
	})
}

// TestMaxIterationsRequiresPositiveCount proves the option itself rejects a
// bound that would either disable it silently or never permit an iteration.
func TestMaxIterationsRequiresPositiveCount(t *testing.T) {
	assertPanicsWith(t, "positive count", func() { duro.WithMaxIterations(0) })
	assertPanicsWith(t, "positive count", func() { duro.WithMaxIterations(-1) })
}

// --- Job identity ----------------------------------------------------------

// TestNewJobRequiresName proves a job cannot be declared nameless.
func TestNewJobRequiresName(t *testing.T) {
	assertPanicsWith(t, "non-empty workflow name", func() { duro.NewJob[int, int]("") })
}

// TestZeroJobRejected proves a Job that did not come from NewJob is refused
// everywhere it is accepted, rather than acting under the empty name.
func TestZeroJobRejected(t *testing.T) {
	var zero duro.Job[int, int]
	assertPanicsWith(t, "requires a job built by NewJob", func() {
		duro.RegisterJob(app, zero, duro.Pipe1(guardStep("never-registered")))
	})
	assertPanicsWith(t, "requires a job built by NewJob", func() {
		_, _ = duro.AttachJob(app, zero, "some-id")
	})
	client := newR3Client(t)
	if _, err := duro.Enqueue(client, r3Queue, zero, 1); err == nil ||
		!strings.Contains(err.Error(), "job built by NewJob") {
		t.Errorf("Enqueue with a zero Job: err = %v, want a NewJob complaint", err)
	}
}

// TestJobNameRoundTrips proves the declaration is the single source of the
// registered name — what RegisterJob registers is what Job.Name reports.
func TestJobNameRoundTrips(t *testing.T) {
	if got := duro.NewJob[int, string]("invoice").Name(); got != "invoice" {
		t.Errorf("Name() = %q, want %q", got, "invoice")
	}
}

// --- Run's single-value contract -------------------------------------------

// guardMultiValuePipeline emits three values, so Run cannot pick one.
func guardMultiValueWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe2(
		duro.Expand("guard-explode", func(_ context.Context, v int) ([]int, error) {
			return []int{v, v + 1, v + 2}, nil
		}),
		duro.Step("guard-double", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
	))
}

// guardSingleValueWorkflow is the same shape folded back to one value, which
// Run must still accept.
func guardSingleValueWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe3(
		duro.Expand("guard-explode-2", func(_ context.Context, v int) ([]int, error) {
			return []int{v, v + 1, v + 2}, nil
		}),
		duro.Step("guard-double-2", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
		duro.Reduce("guard-sum", func(_ context.Context, acc, v int) (int, error) { return acc + v, nil }, 0),
	))
}

// TestRunRejectsMultipleValues proves Run refuses to silently discard results.
// Before this guard the pipeline below returned 8 — the last of 2, 4, 6 — with
// no error and nothing to indicate two results had been dropped.
func TestRunRejectsMultipleValues(t *testing.T) {
	handle, err := dbos.RunWorkflow(dctx, guardMultiValueWorkflow, 1)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil {
		t.Fatal("Run accepted a pipeline emitting 3 values; want ErrMultipleValues")
	}
	if !strings.Contains(err.Error(), "emitted more than one value") {
		t.Errorf("error = %v, want ErrMultipleValues", err)
	}
	// The message must name the remedy, since the fix is a different call.
	if !strings.Contains(err.Error(), "RunAll") {
		t.Errorf("error = %v, want it to point at RunAll", err)
	}
}

// TestRunAcceptsFoldedStream proves the guard does not reject the ordinary
// case: a multi-item stream folded back to one value still runs.
func TestRunAcceptsFoldedStream(t *testing.T) {
	result, _ := mustRun(t, guardSingleValueWorkflow, 1)
	if want := 2 + 4 + 6; result != want {
		t.Errorf("result = %d, want %d", result, want)
	}
}

// --- Loop bound, end to end ------------------------------------------------

var guardLoopBodyRuns atomic.Int64

// guardBoundedLoopWorkflow loops with a predicate that never reports done, so
// only the bound can stop it.
func guardBoundedLoopWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Loop("guard-loop",
			duro.Pipe1(duro.Step("guard-loop-body", func(_ context.Context, v int) (int, error) {
				guardLoopBodyRuns.Add(1)
				return v + 1, nil
			})),
			func(_ context.Context, _ int) (bool, error) { return false, nil },
			duro.WithMaxIterations(guardLoopMax),
		),
	))
}

const guardLoopMax = 4

// TestLoopHonorsMaxIterations proves a bounded Loop stops at exactly its bound
// and fails with ErrMaxIterations, instead of checkpointing forever across
// restarts until someone cancels the workflow by hand.
func TestLoopHonorsMaxIterations(t *testing.T) {
	guardLoopBodyRuns.Store(0)
	handle, err := dbos.RunWorkflow(dctx, guardBoundedLoopWorkflow, 0)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	if _, err = handle.GetResult(); err == nil {
		t.Fatal("bounded Loop completed; want ErrMaxIterations")
	}
	if !strings.Contains(err.Error(), "exceeded its maximum iterations") {
		t.Fatalf("error = %v, want ErrMaxIterations", err)
	}
	if got := guardLoopBodyRuns.Load(); got != guardLoopMax {
		t.Errorf("loop body ran %d times, want exactly %d — the bound must stop it, not overshoot", got, guardLoopMax)
	}
}

// TestUnboundedLoopStillTerminatesNormally proves the bound is opt-in: a Loop
// with no WithMaxIterations is unchanged.
func TestUnboundedLoopStillTerminatesNormally(t *testing.T) {
	result, _ := mustRun(t, guardUnboundedLoopWorkflow, 0)
	if result != 3 {
		t.Errorf("result = %d, want 3", result)
	}
}

func guardUnboundedLoopWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Loop("guard-free-loop",
			duro.Pipe1(duro.Step("guard-free-body", func(_ context.Context, v int) (int, error) {
				return v + 1, nil
			})),
			func(_ context.Context, v int) (bool, error) { return v >= 3, nil },
		),
	))
}

// --- Replay proof: the loop bound is reconstructed, never refreshed ---------

// TestLoopBoundSurvivesReplay is the durability claim behind WithMaxIterations:
// the iteration count is rebuilt by replaying each completed iteration's
// checkpoints, so a recovered run resumes with the budget it had already spent
// rather than a fresh one. Forking mid-loop is a recovery replay: iterations
// before the fork point replay from their checkpoints (their body must not
// re-run), and the run must still stop at the same total iteration.
func TestLoopBoundSurvivesReplay(t *testing.T) {
	guardLoopBodyRuns.Store(0)
	handle, err := dbos.RunWorkflow(dctx, guardBoundedLoopWorkflow, 0)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	originalID := handle.GetWorkflowID()
	if _, err := handle.GetResult(); err == nil {
		t.Fatal("bounded Loop completed; want ErrMaxIterations")
	}
	if got := guardLoopBodyRuns.Load(); got != guardLoopMax {
		t.Fatalf("original run body executions = %d, want %d", got, guardLoopMax)
	}

	// Fork from the body step of the second iteration, so exactly one whole
	// iteration replays from checkpoints. The index is located rather than
	// assumed, so a change in step layout fails loudly instead of silently
	// forking somewhere else.
	names := stepNames(t, originalID)
	forkAt, seen := -1, 0
	for id, name := range names {
		if name != "guard-loop-body" {
			continue
		}
		if seen++; seen == 2 {
			forkAt = id
			break
		}
	}
	if forkAt < 0 {
		t.Fatalf("could not locate the second loop-body step in %v", names)
	}

	guardLoopBodyRuns.Store(0)
	forked, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: originalID,
		StartStep:          uint(forkAt),
	})
	if err != nil {
		t.Fatalf("forking workflow: %v", err)
	}
	if _, err := forked.GetResult(); err == nil {
		t.Fatal("forked run completed; the bound must still stop it")
	} else if !strings.Contains(err.Error(), "exceeded its maximum iterations") {
		t.Fatalf("forked error = %v, want ErrMaxIterations", err)
	}

	// Iteration 1 replayed; iterations 2..guardLoopMax re-executed. A budget
	// that reset on recovery would have run guardLoopMax bodies here instead,
	// performing one extra iteration overall.
	if got, want := guardLoopBodyRuns.Load(), int64(guardLoopMax-1); got != want {
		t.Errorf("body executions after fork = %d, want %d — a replayed iteration re-ran, or the bound reset", got, want)
	}
}

// --- Combinations ----------------------------------------------------------

var guardComboBodyRuns atomic.Int64

// guardLoopInRescueWorkflow composes the new bound with existing control flow:
// a bounded Loop embedded in a Rescue, reached through a Branch arm, driven by
// a multi-item stream. The bound's failure must behave as an ordinary stage
// failure that Rescue can intercept per item, leaving the rest of the stream
// flowing.
//
// The loop body increments and the predicate settles at 3, so an item entering
// at 2 settles in one iteration, an item entering at 0 cannot reach 3 within
// the two-iteration bound and trips it, and a negative item takes the other
// Branch arm and never sees the loop at all.
func guardLoopInRescueWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	bounded := duro.Pipe1(duro.Loop("combo-loop",
		duro.Pipe1(duro.Step("combo-body", func(_ context.Context, v int) (int, error) {
			guardComboBodyRuns.Add(1)
			return v + 1, nil
		})),
		func(_ context.Context, v int) (bool, error) { return v >= 3, nil },
		duro.WithMaxIterations(2),
	))
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("combo-explode", func(_ context.Context, xs []int) ([]int, error) { return xs, nil }),
		duro.Branch("combo-route",
			func(_ context.Context, v int) (bool, error) { return v >= 0, nil },
			duro.Pipe1(duro.Rescue("combo-rescue", bounded,
				func(_ context.Context, _ int, cause error) (int, error) {
					if !strings.Contains(cause.Error(), "exceeded its maximum iterations") {
						return 0, cause
					}
					return -1, nil // the bound tripped: report it as a sentinel
				})),
			duro.Pipe1(duro.Step("combo-negative", func(_ context.Context, v int) (int, error) { return v, nil })),
		),
	))
}

// TestLoopBoundComposesWithRescueAndBranch proves WithMaxIterations behaves as
// an ordinary stage failure inside the existing combinators: Rescue intercepts
// it per item, items that settle within the bound are untouched, and the rest
// of the stream keeps flowing.
func TestLoopBoundComposesWithRescueAndBranch(t *testing.T) {
	guardComboBodyRuns.Store(0)
	result, _ := mustRun(t, guardLoopInRescueWorkflow, []int{2, 0, -5})
	want := []int{3, -1, -5}
	if len(result) != len(want) {
		t.Fatalf("result = %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("result = %v, want %v", result, want)
		}
	}
	// 2 settles in one iteration; 0 burns both before tripping; -5 skips the
	// loop entirely. A bound that failed to stop the loop would run more.
	if got, want := guardComboBodyRuns.Load(), int64(3); got != want {
		t.Errorf("loop body executions = %d, want %d", got, want)
	}
}

func registerGuardWorkflows(ctx dbos.DBOSContext) {
	dbos.RegisterWorkflow(ctx, guardMultiValueWorkflow, dbos.WithWorkflowName("guardMultiValueWorkflow"))
	dbos.RegisterWorkflow(ctx, guardSingleValueWorkflow, dbos.WithWorkflowName("guardSingleValueWorkflow"))
	dbos.RegisterWorkflow(ctx, guardBoundedLoopWorkflow, dbos.WithWorkflowName("guardBoundedLoopWorkflow"))
	dbos.RegisterWorkflow(ctx, guardUnboundedLoopWorkflow, dbos.WithWorkflowName("guardUnboundedLoopWorkflow"))
	dbos.RegisterWorkflow(ctx, guardLoopInRescueWorkflow, dbos.WithWorkflowName("guardLoopInRescueWorkflow"))
}
