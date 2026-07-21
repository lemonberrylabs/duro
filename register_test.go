package duro_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- pipeline-workflow registration test fixtures ---------------------------
// Pipelines are package-level (immutable, stateless); TestMain registers them
// through duro before dbos.Launch, like any DBOS registration.

var registeredWf *duro.PipelineWorkflow[int, int]

var registeredPipeline = duro.Pipe2(
	duro.Step("double", func(_ context.Context, v int) (int, error) { return v * 2, nil }),
	duro.Step("plus-one", func(_ context.Context, v int) (int, error) { return v + 1, nil }),
)

var (
	cronTicks  atomic.Int64
	cronLastID atomic.Value // string: workflow ID of the latest scheduled run
)

var scheduledPipeline = duro.Pipe1(
	duro.Step("tick", func(stepCtx context.Context, ts time.Time) (string, error) {
		cronTicks.Add(1)
		if dctx, ok := stepCtx.(dbos.DBOSContext); ok {
			if id, err := dbos.GetWorkflowID(dctx); err == nil {
				cronLastID.Store(id)
			}
		}
		return ts.UTC().Format(time.RFC3339), nil
	}),
)

var (
	debouncedRuns atomic.Int64
	debouncer     *duro.Debouncer[int, int]
)

var debouncedPipeline = duro.Pipe1(
	duro.Step("run-once", func(_ context.Context, v int) (int, error) {
		debouncedRuns.Add(1)
		return v * 100, nil
	}),
)

// handWrittenSum is a hand-written workflow: imperative code around a
// pipeline, using RunAll to keep every emitted value.
func handWrittenSum(ctx duro.Context, ns []int) (int, error) {
	squares, err := duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.Step("square", func(_ context.Context, v int) (int, error) { return v * v, nil }),
	))
	if err != nil {
		return 0, err
	}
	sum := 0
	for _, v := range squares {
		sum += v
	}
	return sum, nil
}

var (
	handWrittenWf *duro.RegisteredWorkflow[[]int, int]
	fanIntoFnWf   *duro.PipelineWorkflow[[][]int, []int]
)

func registerPipelineWorkflows(ctx dbos.DBOSContext) {
	registeredWf = duro.Register(ctx, "registeredPipeline", registeredPipeline)
	duro.RegisterScheduled(ctx, "cronPipeline", "* * * * * *", scheduledPipeline) // every second
	debouncer = duro.RegisterDebounced(ctx, "debouncedPipeline", debouncedPipeline)

	// Deliberately registered through the App (ctx here is *duro.App in
	// TestMain): duro.RegisterWorkflow must unwrap it — the raw
	// dbos.RegisterWorkflow would silently no-op on the App.
	handWrittenWf = duro.RegisterWorkflow(ctx, "handWrittenSum", handWrittenSum)
	fanIntoFnWf = duro.Register(ctx, "fanIntoFn", duro.Pipe3(
		duro.Expand("explode-batches", func(_ context.Context, batches [][]int) ([][]int, error) {
			return batches, nil
		}),
		duro.FanOut("fan", fanQueue, handWrittenWf),
		collectInts(),
	))
}

// --- tests ------------------------------------------------------------------

// TestRegisterPipelineAsWorkflow proves Register yields a runnable DBOS
// workflow under the given name whose body is the checkpointed pipeline.
func TestRegisterPipelineAsWorkflow(t *testing.T) {
	handle, err := registeredWf.Start(app, 5)
	if err != nil {
		t.Fatalf("starting registered pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("registered pipeline failed: %v", err)
	}
	wfID := handle.ID()
	if result != 11 {
		t.Errorf("result = %d, want 11", result)
	}
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "double", "plus-one"})

	status, err := handle.Status()
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if status.Name != "registeredPipeline" {
		t.Errorf("workflow name = %q, want %q", status.Name, "registeredPipeline")
	}
}

// TestScheduledPipeline proves RegisterScheduled runs the pipeline on the
// cron schedule as durable workflows: a tick fires within a few seconds and
// its run checkpoints the stages like any other pipeline.
func TestScheduledPipeline(t *testing.T) {
	deadline := time.Now().Add(15 * time.Second)
	for cronTicks.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no scheduled run within 15s")
		}
		time.Sleep(100 * time.Millisecond)
	}

	id, _ := cronLastID.Load().(string)
	if id == "" {
		t.Fatal("scheduled run did not record its workflow ID")
	}
	handle, err := dbos.RetrieveWorkflow[string](dctx, id)
	if err != nil {
		t.Fatalf("retrieving scheduled run %s: %v", id, err)
	}
	if _, err := handle.GetResult(); err != nil {
		t.Fatalf("scheduled run failed: %v", err)
	}
	assertNames(t, stepNames(t, id), []string{duro.ShapeStepName, "tick"})
}

// TestDebouncedPipeline proves RegisterDebounced collapses a burst of
// triggers into one run with the last input.
func TestDebouncedPipeline(t *testing.T) {
	debouncedRuns.Store(0)

	const key = "debounce-test"
	h1, err := debouncer.Debounce(app, key, 400*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("first debounce: %v", err)
	}
	if _, err = debouncer.Debounce(app, key, 400*time.Millisecond, 2); err != nil {
		t.Fatalf("second debounce: %v", err)
	}
	h3, err := debouncer.Debounce(app, key, 400*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("third debounce: %v", err)
	}

	result, err := h3.Result()
	if err != nil {
		t.Fatalf("debounced workflow failed: %v", err)
	}
	if result != 300 {
		t.Errorf("result = %d, want 300 (last input wins)", result)
	}
	if first, err := h1.Result(); err != nil || first != result {
		t.Errorf("first handle result = %d (%v), want the same single run's %d", first, err, result)
	}
	if h1.ID() != h3.ID() {
		t.Errorf("burst produced different workflows: %s vs %s", h1.ID(), h3.ID())
	}
	if got := debouncedRuns.Load(); got != 1 {
		t.Errorf("pipeline executions = %d, want 1", got)
	}
}

// TestRegisterWorkflow proves hand-written workflows register, start, and
// carry their durable name without any direct dbos call — including when the
// registration context is the App itself.
func TestRegisterWorkflow(t *testing.T) {
	handle, err := handWrittenWf.Start(app, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("starting hand-written workflow: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("hand-written workflow failed: %v", err)
	}
	if result != 14 {
		t.Errorf("result = %d, want 14 (1+4+9)", result)
	}
	status, err := handle.Status()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Name != "handWrittenSum" {
		t.Errorf("workflow name = %q, want %q", status.Name, "handWrittenSum")
	}
}

// TestRegisteredWorkflowAsFanOutChild proves a *RegisteredWorkflow is a
// WorkflowRef: FanOut takes it directly, no duro.Workflow adapter.
func TestRegisteredWorkflowAsFanOutChild(t *testing.T) {
	handle, err := fanIntoFnWf.Start(app, [][]int{{1, 2}, {3}})
	if err != nil {
		t.Fatalf("starting parent: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("parent failed: %v", err)
	}
	assertInts(t, result, []int{5, 9}) // 1+4, 9
}
