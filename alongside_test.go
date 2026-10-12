package duro_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lemonberrylabs/duro"
)

// --- Alongside test pipelines --------------------------------------------------

// alongsideMeeting is the rendezvous the concurrency test uses: each half
// announces itself and waits for the other, so the run only succeeds when the
// side step and the embedded pipeline are in flight at the same time.
type alongsideMeeting struct {
	side, main chan struct{}
}

var (
	alongsideRendezvous atomic.Pointer[alongsideMeeting]

	alongsideSideRuns   atomic.Int64
	alongsideMainRuns   atomic.Int64
	alongsideAfterRuns  atomic.Int64
	alongsideRescueRuns atomic.Int64
	alongsideSideDone   atomic.Bool
	alongsideCause      atomic.Value // string: the cause the rescue handler saw
)

func resetAlongsideCounters() {
	alongsideSideRuns.Store(0)
	alongsideMainRuns.Store(0)
	alongsideAfterRuns.Store(0)
	alongsideRescueRuns.Store(0)
	alongsideSideDone.Store(false)
	alongsideCause.Store("")
}

func alongsideMeet(mine, theirs chan struct{}) error {
	close(mine)
	select {
	case <-theirs:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("the other half never started: not concurrent")
	}
}

func alongsideRethrow(_ context.Context, _ int, cause error) error { return cause }

func alongsideSwallow(_ context.Context, _ int, cause error) error {
	alongsideRescueRuns.Add(1)
	alongsideCause.Store(cause.Error())
	return nil
}

func alongsideFailingSide(_ context.Context, _ int) error {
	alongsideSideRuns.Add(1)
	return errors.New("side boom")
}

func alongsideAfter(_ context.Context, v int) (int, error) {
	alongsideAfterRuns.Add(1)
	return v + 1, nil
}

func alongsideMain(_ context.Context, _ int) error {
	alongsideMainRuns.Add(1)
	return nil
}

// alongsideFanQueue is only referenced inside an Alongside-embedded pipeline;
// registering the pipeline must still auto-register it.
var alongsideFanQueue = duro.NewQueue("duro-test-alongside-fan", duro.WithConcurrency(2))

var (
	alongsideWf         *duro.PipelineWorkflow[int, int]
	alongsideSwallowWf  *duro.PipelineWorkflow[int, int]
	alongsideRethrowWf  *duro.PipelineWorkflow[int, int]
	alongsideMainFailWf *duro.PipelineWorkflow[int, int]
	alongsideFanWf      *duro.PipelineWorkflow[[]int, []int]
)

func registerAlongsideWorkflows(app *duro.App) {
	alongsideWf = duro.Register(app, "alongsidePipeline", duro.Pipe2(
		duro.Alongside("side", func(_ context.Context, _ int) error {
			alongsideSideRuns.Add(1)
			m := alongsideRendezvous.Load()
			return alongsideMeet(m.side, m.main)
		}, alongsideRethrow, duro.Pipe2(
			duro.Tap("main", func(_ context.Context, _ int) error {
				alongsideMainRuns.Add(1)
				m := alongsideRendezvous.Load()
				return alongsideMeet(m.main, m.side)
			}),
			// A value that must NOT appear downstream.
			duro.Step("embedded-out", func(_ context.Context, v int) (int, error) { return v * 100, nil }),
		)),
		duro.Step("after", alongsideAfter),
	))

	alongsideSwallowWf = duro.Register(app, "alongsideSwallowPipeline", duro.Pipe2(
		duro.Alongside("side", alongsideFailingSide, alongsideSwallow,
			duro.Pipe1(duro.Tap("main", alongsideMain)),
			duro.WithMaxRetries(2), duro.WithBaseInterval(time.Millisecond)),
		duro.Step("after", alongsideAfter),
	))

	alongsideRethrowWf = duro.Register(app, "alongsideRethrowPipeline", duro.Pipe2(
		duro.Alongside("side", alongsideFailingSide,
			func(_ context.Context, _ int, cause error) error { return fmt.Errorf("rethrown: %w", cause) },
			duro.Pipe1(duro.Tap("main", alongsideMain))),
		duro.Step("after", alongsideAfter),
	))

	alongsideMainFailWf = duro.Register(app, "alongsideMainFailPipeline", duro.Pipe2(
		duro.Alongside("side", func(_ context.Context, _ int) error {
			time.Sleep(300 * time.Millisecond)
			alongsideSideDone.Store(true)
			return errors.New("side boom")
		}, alongsideSwallow, duro.Pipe1(
			duro.Tap("main", func(_ context.Context, _ int) error { return errors.New("main boom") }),
		)),
		duro.Step("after", alongsideAfter),
	))

	alongsideFanWf = duro.Register(app, "alongsideFanPipeline", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.Rescue("guard", duro.Pipe1(
			duro.Alongside("side", func(_ context.Context, _ int) error {
				alongsideSideRuns.Add(1)
				return nil
			}, alongsideRethrow, duro.Pipe1(
				duro.FanOut("fan", alongsideFanQueue, duro.Workflow(fanChildSquareOrFail)),
			)),
		), func(_ context.Context, in int, _ error) (int, error) {
			alongsideRescueRuns.Add(1)
			return in, nil
		}),
		duro.Collect[int]("collect"),
	))
}

func runAlongside[P, R any](t *testing.T, wf *duro.PipelineWorkflow[P, R], in P) (R, string, error) {
	t.Helper()
	handle, err := wf.Start(app, in)
	if err != nil {
		t.Fatalf("starting pipeline: %v", err)
	}
	result, err := handle.Result()
	return result, handle.ID(), err
}

// --- Alongside -----------------------------------------------------------------

// TestAlongsideRunsSideConcurrently proves the side step and the embedded
// pipeline are in flight together, the original item comes out the other
// side, and the side step takes the first step slot. A fork from the stage
// after it then proves both replay from their checkpoints.
func TestAlongsideRunsSideConcurrently(t *testing.T) {
	resetAlongsideCounters()
	alongsideRendezvous.Store(&alongsideMeeting{side: make(chan struct{}), main: make(chan struct{})})

	result, wfID, err := runAlongside(t, alongsideWf, 5)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if result != 6 {
		t.Errorf("result = %d, want 6 (the original item, not the embedded pipeline's 500)", result)
	}
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "side", "main", "embedded-out", "after"})

	handle, err := duro.ForkFromStage[int](app, duro.Fork{WorkflowID: wfID, Stage: "after"})
	if err != nil {
		t.Fatalf("forking: %v", err)
	}
	forked, err := handle.Result()
	if err != nil {
		t.Fatalf("forked run failed: %v", err)
	}
	if forked != 6 {
		t.Errorf("forked result = %d, want 6", forked)
	}
	if side, main, after := alongsideSideRuns.Load(), alongsideMainRuns.Load(), alongsideAfterRuns.Load(); side != 1 || main != 1 || after != 2 {
		t.Errorf("executions side=%d main=%d after=%d, want 1/1/2 (only the forked stage re-runs)", side, main, after)
	}
}

// TestAlongsideRescueSwallowsSideFailure proves a side failure reaches the
// handler only after the step's own retries, as a plain error carrying the
// step's message, and that a swallowed failure lets the item continue. The
// fork proves the rescue decision is a checkpoint: it is not re-made.
func TestAlongsideRescueSwallowsSideFailure(t *testing.T) {
	resetAlongsideCounters()

	result, wfID, err := runAlongside(t, alongsideSwallowWf, 5)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}
	if got := alongsideSideRuns.Load(); got != 3 {
		t.Errorf("side attempts = %d, want 3 (one try and two retries)", got)
	}
	if got := alongsideRescueRuns.Load(); got != 1 {
		t.Errorf("rescue executions = %d, want 1", got)
	}
	if cause, _ := alongsideCause.Load().(string); !strings.Contains(cause, "side boom") {
		t.Errorf("rescue cause = %q, want the side step's message", cause)
	}
	// The handler's checkpoint carries the stage's name, after the embedded steps.
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "side", "main", "side", "after"})

	handle, err := duro.ForkFromStage[int](app, duro.Fork{WorkflowID: wfID, Stage: "after"})
	if err != nil {
		t.Fatalf("forking: %v", err)
	}
	if _, err := handle.Result(); err != nil {
		t.Fatalf("forked run failed: %v", err)
	}
	if side, rescue := alongsideSideRuns.Load(), alongsideRescueRuns.Load(); side != 3 || rescue != 1 {
		t.Errorf("after the fork: side attempts=%d rescue=%d, want 3/1 (both replayed)", side, rescue)
	}
}

// TestAlongsideRescueRethrows proves the handler's error fails the outer
// pipeline and the stage behind it never runs.
func TestAlongsideRescueRethrows(t *testing.T) {
	resetAlongsideCounters()

	_, _, err := runAlongside(t, alongsideRethrowWf, 5)
	if err == nil || !strings.Contains(err.Error(), "rethrown") || !strings.Contains(err.Error(), "side boom") {
		t.Fatalf("pipeline error = %v, want the handler's rethrown side failure", err)
	}
	if got := alongsideAfterRuns.Load(); got != 0 {
		t.Errorf("after-stage executions = %d, want 0", got)
	}
}

// TestAlongsideEmbeddedFailureWins proves an embedded failure fails the stage
// with its own error even when the side step fails too, that the handler does
// not run, and that the run does not return while the side step is in flight.
func TestAlongsideEmbeddedFailureWins(t *testing.T) {
	resetAlongsideCounters()

	_, _, err := runAlongside(t, alongsideMainFailWf, 5)
	if err == nil || !strings.Contains(err.Error(), "main boom") {
		t.Fatalf("pipeline error = %v, want the embedded failure", err)
	}
	if !alongsideSideDone.Load() {
		t.Error("the run failed while the side step was still running")
	}
	if got := alongsideRescueRuns.Load(); got != 0 {
		t.Errorf("rescue executions = %d, want 0 (an embedded failure is not the handler's)", got)
	}
	if got := alongsideAfterRuns.Load(); got != 0 {
		t.Errorf("after-stage executions = %d, want 0", got)
	}
}

// TestAlongsideEmbeddedFanOutInRescue composes the stage with the rest: per
// item on a multi-item stream, a FanOut on a queue referenced only inside it,
// under a Rescue that swallows the embedded failure of one item.
func TestAlongsideEmbeddedFanOutInRescue(t *testing.T) {
	resetAlongsideCounters()

	result, _, err := runAlongside(t, alongsideFanWf, []int{1, -2, 3})
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	assertInts(t, result, []int{1, -2, 3})
	if got := alongsideSideRuns.Load(); got != 3 {
		t.Errorf("side executions = %d, want 3 (one per item)", got)
	}
	if got := alongsideRescueRuns.Load(); got != 1 {
		t.Errorf("outer rescue executions = %d, want 1 (the item whose child failed)", got)
	}
}

// TestAlongsideConstructionGuards proves misuse fails at construction.
func TestAlongsideConstructionGuards(t *testing.T) {
	side := func(_ context.Context, _ int) error { return nil }
	pass := duro.Pipe1(guardStep("id"))

	assertPanics(t, "nil side function", func() { duro.Alongside("a", nil, alongsideRethrow, pass) })
	assertPanics(t, "nil rescue handler", func() { duro.Alongside("a", side, nil, pass) })
	assertPanics(t, "zero-value pipeline", func() {
		duro.Alongside("a", side, alongsideRethrow, duro.Pipeline[int, int]{})
	})
	assertPanicsWith(t, "applies only to Parallel", func() {
		duro.Alongside("a", side, alongsideRethrow, pass, duro.WithCancelSiblingSteps())
	})
	assertPanicsWith(t, "applies only to Loop", func() {
		duro.Alongside("a", side, alongsideRethrow, pass, duro.WithMaxIterations(3))
	})
}
