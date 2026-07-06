package duro_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/samber/ro"

	"github.com/lemonberrylabs/duro"
)

// The tests run against a real local Postgres so that checkpointing, replay,
// and forking exercise the same machinery as production DBOS. The dbos schema
// in the test database is dropped before each `go test` run.

var dctx dbos.DBOSContext

func TestMain(m *testing.M) {
	url := testDatabaseURL()
	if err := wipeSystemSchema(url); err != nil {
		fmt.Fprintf(os.Stderr, "wiping dbos schema in %s: %v\n", url, err)
		os.Exit(1)
	}

	ctx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:     "duro-test",
		DatabaseURL: url,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "initializing DBOS: %v\n", err)
		os.Exit(1)
	}

	dbos.RegisterWorkflow(ctx, linearWorkflow, dbos.WithWorkflowName("linearWorkflow"))
	dbos.RegisterWorkflow(ctx, plainLinearWorkflow, dbos.WithWorkflowName("plainLinearWorkflow"))
	dbos.RegisterWorkflow(ctx, flakyWorkflow, dbos.WithWorkflowName("flakyWorkflow"))
	dbos.RegisterWorkflow(ctx, midstreamFailureWorkflow, dbos.WithWorkflowName("midstreamFailureWorkflow"))
	dbos.RegisterWorkflow(ctx, complexPipelineWorkflow, dbos.WithWorkflowName("complexPipelineWorkflow"))
	dbos.RegisterWorkflow(ctx, dropAllWorkflow, dbos.WithWorkflowName("dropAllWorkflow"))
	dbos.RegisterWorkflow(ctx, multiValueWorkflow, dbos.WithWorkflowName("multiValueWorkflow"))
	dbos.RegisterWorkflow(ctx, mutableShapeWorkflow, dbos.WithWorkflowName("mutableShapeWorkflow"))
	dbos.RegisterWorkflow(ctx, goroutineShiftWorkflow, dbos.WithWorkflowName("goroutineShiftWorkflow"))

	if err := dbos.Launch(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "launching DBOS: %v\n", err)
		os.Exit(1)
	}
	dctx = ctx

	code := m.Run()
	dbos.Shutdown(ctx, 10*time.Second)
	os.Exit(code)
}

func testDatabaseURL() string {
	if url := os.Getenv("DURO_TEST_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_test", username)
}

func wipeSystemSchema(url string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "DROP SCHEMA IF EXISTS dbos CASCADE")
	return err
}

// --- test workflows -------------------------------------------------------
// Execution counters distinguish "stage function actually ran" from "step
// result replayed from a checkpoint" — the heart of the durability assertions.

var (
	doubleCount    atomic.Int64
	addThreeCount  atomic.Int64
	stringifyCount atomic.Int64

	flakyAttempts atomic.Int64

	squareBeforeRejectCount atomic.Int64
	rejectCount             atomic.Int64
	afterRejectCount        atomic.Int64

	explodeCount atomic.Int64
	filterCount  atomic.Int64
	squareCount  atomic.Int64
	auditCount   atomic.Int64
	sumCount     atomic.Int64
)

func linearWorkflow(ctx dbos.DBOSContext, n int) (string, error) {
	return duro.Run(ctx, n, duro.Pipe3(
		duro.Step("double", func(_ context.Context, v int) (int, error) {
			doubleCount.Add(1)
			return v * 2, nil
		}),
		duro.Step("add-three", func(_ context.Context, v int) (int, error) {
			addThreeCount.Add(1)
			return v + 3, nil
		}),
		duro.Step("stringify", func(_ context.Context, v int) (string, error) {
			stringifyCount.Add(1)
			return fmt.Sprintf("value=%d", v), nil
		}),
	))
}

// plainLinearWorkflow performs the same computation as linearWorkflow with
// sequential RunAsStep calls, for step-sequence parity checks.
func plainLinearWorkflow(ctx dbos.DBOSContext, n int) (string, error) {
	doubled, err := dbos.RunAsStep(ctx, func(context.Context) (int, error) {
		return n * 2, nil
	}, dbos.WithStepName("double"))
	if err != nil {
		return "", err
	}
	added, err := dbos.RunAsStep(ctx, func(context.Context) (int, error) {
		return doubled + 3, nil
	}, dbos.WithStepName("add-three"))
	if err != nil {
		return "", err
	}
	return dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		return fmt.Sprintf("value=%d", added), nil
	}, dbos.WithStepName("stringify"))
}

func flakyWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Step("flaky", func(_ context.Context, v int) (int, error) {
			if flakyAttempts.Add(1) < 3 {
				return 0, errors.New("simulated transient failure")
			}
			return v * 10, nil
		}, duro.WithMaxRetries(3), duro.WithBaseInterval(time.Millisecond)),
	))
}

// midstreamFailureWorkflow expands into three items and fails on the second,
// verifying that a mid-stream error stops the pipeline: the third item must
// never reach any stage.
func midstreamFailureWorkflow(ctx dbos.DBOSContext, _ int) (int, error) {
	return duro.Run(ctx, []int{1, 2, 3}, duro.Pipe4(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.Step("square", func(_ context.Context, v int) (int, error) {
			squareBeforeRejectCount.Add(1)
			return v * v, nil
		}),
		duro.Step("reject-four", func(_ context.Context, v int) (int, error) {
			rejectCount.Add(1)
			if v == 4 {
				return 0, errors.New("boom: four is not allowed")
			}
			return v, nil
		}),
		duro.Step("after", func(_ context.Context, v int) (int, error) {
			afterRejectCount.Add(1)
			return v, nil
		}),
	))
}

// complexPipelineWorkflow is the full primitive set in one durable pipeline:
// Expand → Filter → Step → Tap → Reduce, with a Pure reshaping stage that is
// never checkpointed. For input [1 2 3 4 5] it keeps the odd values, squares
// them, and sums to 35.
func complexPipelineWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe6(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			explodeCount.Add(1)
			return xs, nil
		}),
		duro.Filter("odd-only", func(_ context.Context, v int) (bool, error) {
			filterCount.Add(1)
			return v%2 == 1, nil
		}),
		duro.Pure("identity", func(v int) int { return v }),
		duro.Step("square", func(_ context.Context, v int) (int, error) {
			squareCount.Add(1)
			return v * v, nil
		}),
		duro.Tap("audit", func(_ context.Context, _ int) error {
			auditCount.Add(1)
			return nil
		}),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			sumCount.Add(1)
			return acc + v, nil
		}, 0),
	))
}

func dropAllWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe1(
		duro.Filter("drop-all", func(_ context.Context, _ int) (bool, error) {
			return false, nil
		}),
	))
}

func multiValueWorkflow(ctx dbos.DBOSContext, n int) ([]int, error) {
	return duro.RunAll(ctx, n, duro.Pipe2(
		duro.Expand("one-to-n", func(_ context.Context, v int) ([]int, error) {
			out := make([]int, v)
			for i := range out {
				out[i] = i + 1
			}
			return out, nil
		}),
		duro.Step("square", func(_ context.Context, v int) (int, error) {
			return v * v, nil
		}),
	))
}

// includeExtraStage simulates non-deterministic pipeline construction (or a
// code change between the original run and a replay) for the shape-guard
// test. Tests run sequentially, so a plain bool is fine.
var includeExtraStage = false

func mutableShapeWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	double := duro.Step("double", func(_ context.Context, v int) (int, error) {
		return v * 2, nil
	})
	if includeExtraStage {
		return duro.Run(ctx, n, duro.Pipe2(
			double,
			duro.Step("extra", func(_ context.Context, v int) (int, error) {
				return v + 1, nil
			}),
		))
	}
	return duro.Run(ctx, n, duro.Pipe1(double))
}

// goroutineShift re-emits values from a fresh goroutine — the kind of
// concurrency a raw ro operator could introduce. Only expressible through
// UnsafeOperator; the runtime goroutine guard must reject the next stage.
func goroutineShift[T any]() duro.Stage[T, T] {
	return duro.UnsafeOperator("goroutine-shift", func(source ro.Observable[T]) ro.Observable[T] {
		return ro.NewUnsafeObservableWithContext(func(ctx context.Context, dest ro.Observer[T]) ro.Teardown {
			var items []T
			source.SubscribeWithContext(ctx, ro.NewObserverWithContext(
				func(_ context.Context, v T) { items = append(items, v) },
				dest.ErrorWithContext,
				func(c context.Context) {
					go func() {
						for _, v := range items {
							dest.NextWithContext(c, v)
						}
						dest.CompleteWithContext(c)
					}()
				},
			))
			return nil
		})
	})
}

func goroutineShiftWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe3(
		duro.Step("before", func(_ context.Context, v int) (int, error) {
			return v + 1, nil
		}),
		goroutineShift[int](),
		duro.Step("after-shift", func(_ context.Context, v int) (int, error) {
			return v * 2, nil
		}),
	))
}

// --- helpers ---------------------------------------------------------------

func mustRun[P, R any](t *testing.T, wf dbos.Workflow[P, R], input P, opts ...dbos.WorkflowOption) (R, string) {
	t.Helper()
	handle, err := dbos.RunWorkflow(dctx, wf, input, opts...)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	return result, handle.GetWorkflowID()
}

func stepNames(t *testing.T, workflowID string) []string {
	t.Helper()
	steps, err := dbos.GetWorkflowSteps(dctx, workflowID)
	if err != nil {
		t.Fatalf("fetching steps for %s: %v", workflowID, err)
	}
	names := make([]string, len(steps))
	for _, s := range steps {
		if s.StepID < 0 || s.StepID >= len(steps) {
			t.Fatalf("step ID %d out of range for %d steps", s.StepID, len(steps))
		}
		names[s.StepID] = s.StepName
	}
	return names
}

func assertNames(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("recorded %d steps %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d is %q, want %q (full sequence %v)", i, got[i], want[i], got)
		}
	}
}

// --- tests -----------------------------------------------------------------

func TestLinearPipeline(t *testing.T) {
	doubleCount.Store(0)
	addThreeCount.Store(0)
	stringifyCount.Store(0)

	result, wfID := mustRun(t, linearWorkflow, 5)
	if result != "value=13" {
		t.Errorf("result = %q, want %q", result, "value=13")
	}
	if doubleCount.Load() != 1 || addThreeCount.Load() != 1 || stringifyCount.Load() != 1 {
		t.Errorf("stage executions = %d/%d/%d, want 1/1/1", doubleCount.Load(), addThreeCount.Load(), stringifyCount.Load())
	}
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "double", "add-three", "stringify"})
}

// TestPlainParity proves the duro pipeline checkpoints exactly like
// handwritten sequential RunAsStep calls: same result, same recorded step
// sequence apart from the leading duro.shape bookkeeping step.
func TestPlainParity(t *testing.T) {
	roResult, roID := mustRun(t, linearWorkflow, 8)
	plainResult, plainID := mustRun(t, plainLinearWorkflow, 8)

	if roResult != plainResult {
		t.Errorf("duro result %q != plain result %q", roResult, plainResult)
	}
	roNames := stepNames(t, roID)
	if roNames[0] != duro.ShapeStepName {
		t.Fatalf("first duro step is %q, want %q", roNames[0], duro.ShapeStepName)
	}
	assertNames(t, roNames[1:], stepNames(t, plainID))
}

func TestStepRetries(t *testing.T) {
	flakyAttempts.Store(0)

	result, wfID := mustRun(t, flakyWorkflow, 7)
	if result != 70 {
		t.Errorf("result = %d, want 70", result)
	}
	if got := flakyAttempts.Load(); got != 3 {
		t.Errorf("flaky stage attempts = %d, want 3", got)
	}

	// Retries are internal to the step: exactly one successful step recorded.
	names := stepNames(t, wfID)
	assertNames(t, names, []string{duro.ShapeStepName, "flaky"})
}

func TestMidstreamErrorStopsPipeline(t *testing.T) {
	squareBeforeRejectCount.Store(0)
	rejectCount.Store(0)
	afterRejectCount.Store(0)

	handle, err := dbos.RunWorkflow(dctx, midstreamFailureWorkflow, 0)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "four is not allowed") {
		t.Fatalf("workflow error = %v, want the reject-four failure", err)
	}

	// Item 1 passes all stages, item 2 fails at reject-four, item 3 must be
	// dropped before reaching any stage.
	if got := squareBeforeRejectCount.Load(); got != 2 {
		t.Errorf("square executions = %d, want 2 (items 1 and 2 only)", got)
	}
	if got := rejectCount.Load(); got != 2 {
		t.Errorf("reject-four executions = %d, want 2", got)
	}
	if got := afterRejectCount.Load(); got != 1 {
		t.Errorf("after executions = %d, want 1 (item 1 only)", got)
	}
}

func TestComplexPipeline(t *testing.T) {
	resetComplexCounters()

	result, wfID := mustRun(t, complexPipelineWorkflow, []int{1, 2, 3, 4, 5})
	if result != 35 {
		t.Errorf("result = %d, want 35 (1² + 3² + 5²)", result)
	}
	if explodeCount.Load() != 1 || filterCount.Load() != 5 || squareCount.Load() != 3 || auditCount.Load() != 3 || sumCount.Load() != 3 {
		t.Errorf("executions explode/filter/square/audit/sum = %d/%d/%d/%d/%d, want 1/5/3/3/3",
			explodeCount.Load(), filterCount.Load(), squareCount.Load(), auditCount.Load(), sumCount.Load())
	}
	assertNames(t, stepNames(t, wfID), complexPipelineStepSequence)
}

// complexPipelineStepSequence is the exact checkpoint order for input
// [1 2 3 4 5]: the shape bookkeeping step, then items flowing one at a time
// through the whole pipe, only odd items passing the filter. The Pure
// "identity" stage never appears — it is not checkpointed.
var complexPipelineStepSequence = []string{
	duro.ShapeStepName,
	"explode",
	"odd-only", "square", "audit", "sum", // 1
	"odd-only",                           // 2
	"odd-only", "square", "audit", "sum", // 3
	"odd-only",                           // 4
	"odd-only", "square", "audit", "sum", // 5
}

// TestDurabilityRerunSameWorkflowID proves that re-running a completed
// workflow ID returns the recorded result without executing any stage
// function again.
func TestDurabilityRerunSameWorkflowID(t *testing.T) {
	resetComplexCounters()
	const wfID = "durability-rerun"

	first, _ := mustRun(t, complexPipelineWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	executionsAfterFirst := complexExecutionCounts()

	second, _ := mustRun(t, complexPipelineWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	if first != second {
		t.Errorf("rerun result = %d, want recorded result %d", second, first)
	}
	if got := complexExecutionCounts(); got != executionsAfterFirst {
		t.Errorf("stage executions changed on rerun: %v → %v (steps re-executed instead of replayed)", executionsAfterFirst, got)
	}
}

// TestForkReplaysCheckpointedSteps simulates mid-workflow crash recovery
// in-process: forking from step 7 re-executes the workflow function from
// scratch, replaying steps 0–6 from the original run's checkpoints and
// executing only steps 7–15. The pipeline must route replayed values through
// the observable chain without re-running the stage functions.
func TestForkReplaysCheckpointedSteps(t *testing.T) {
	resetComplexCounters()

	original, originalID := mustRun(t, complexPipelineWorkflow, []int{1, 2, 3, 4, 5})
	baseline := complexExecutionCounts()
	if want := (complexCounts{1, 5, 3, 3, 3}); baseline != want {
		t.Fatalf("baseline executions = %v, want %v", baseline, want)
	}

	handle, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: originalID,
		StartStep:          7, // first stage execution for item 3
	})
	if err != nil {
		t.Fatalf("forking workflow: %v", err)
	}
	forked, err := handle.GetResult()
	if err != nil {
		t.Fatalf("forked workflow failed: %v", err)
	}
	if forked != original {
		t.Errorf("forked result = %d, want %d", forked, original)
	}

	// Steps 0-6 (shape; explode; item 1's odd-only/square/audit/sum; item 2's
	// odd-only) replay from checkpoints; steps 7-15 execute.
	got := complexExecutionCounts()
	want := complexCounts{
		explode: baseline.explode + 0,
		filter:  baseline.filter + 3, // items 3, 4, 5
		square:  baseline.square + 2, // items 3, 5
		audit:   baseline.audit + 2,
		sum:     baseline.sum + 2,
	}
	if got != want {
		t.Errorf("executions after fork = %v, want %v", got, want)
	}
	assertNames(t, stepNames(t, handle.GetWorkflowID()), complexPipelineStepSequence)
}

// TestShapeMismatchFailsFast proves the construction-time guard: a replay
// that constructs a different pipeline shape fails at duro.Run, before any
// stage can read a misaligned checkpoint. ForkWorkflow from step 1 replays
// the recorded duro.shape checkpoint while re-executing everything else —
// exactly a recovery replay of the (now different) workflow code.
func TestShapeMismatchFailsFast(t *testing.T) {
	includeExtraStage = false
	result, originalID := mustRun(t, mutableShapeWorkflow, 21)
	if result != 42 {
		t.Fatalf("result = %d, want 42", result)
	}

	includeExtraStage = true
	defer func() { includeExtraStage = false }()

	handle, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: originalID,
		StartStep:          1,
	})
	if err != nil {
		t.Fatalf("forking workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "pipeline shape mismatch") {
		t.Fatalf("forked workflow error = %v, want a pipeline shape mismatch", err)
	}
}

// TestGoroutineShiftFailsFast proves the execution-time guard: an
// UnsafeOperator that moves emissions onto another goroutine causes the next
// durable stage to fail with a clear error instead of racing DBOS's step
// counter and corrupting checkpoint order.
func TestGoroutineShiftFailsFast(t *testing.T) {
	handle, err := dbos.RunWorkflow(dctx, goroutineShiftWorkflow, 1)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "would break deterministic step ordering") {
		t.Fatalf("workflow error = %v, want the goroutine guard error", err)
	}
}

// TestRunOutsideWorkflowFails proves stages refuse to run when the
// DBOSContext is not executing a workflow (no checkpointing possible).
func TestRunOutsideWorkflowFails(t *testing.T) {
	_, err := duro.Run(dctx, 1, duro.Pipe1(
		duro.Step("double", func(_ context.Context, v int) (int, error) {
			return v * 2, nil
		}),
	))
	if err == nil || !strings.Contains(err.Error(), "workflow") {
		t.Errorf("error = %v, want a not-within-a-workflow error", err)
	}
}

func TestConstructionPanics(t *testing.T) {
	assertPanics(t, "empty name", func() {
		duro.Step("", func(_ context.Context, v int) (int, error) { return v, nil })
	})
	assertPanics(t, "nil function", func() {
		duro.Step[int, int]("named", nil)
	})
	assertPanics(t, "nil pure function", func() {
		duro.Pure[int, int]("reshape", nil)
	})
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected a construction panic", name)
		}
	}()
	fn()
}

func TestEmptyPipelineReturnsErrNoValue(t *testing.T) {
	handle, err := dbos.RunWorkflow(dctx, dropAllWorkflow, 42)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "without emitting a value") {
		t.Errorf("error = %v, want ErrNoValue", err)
	}
}

func TestRunAllCollectsAllValues(t *testing.T) {
	result, wfID := mustRun(t, multiValueWorkflow, 4)
	want := []int{1, 4, 9, 16}
	if len(result) != len(want) {
		t.Fatalf("result = %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("result = %v, want %v", result, want)
		}
	}
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "one-to-n", "square", "square", "square", "square"})
}

type complexCounts struct {
	explode, filter, square, audit, sum int64
}

func complexExecutionCounts() complexCounts {
	return complexCounts{
		explode: explodeCount.Load(),
		filter:  filterCount.Load(),
		square:  squareCount.Load(),
		audit:   auditCount.Load(),
		sum:     sumCount.Load(),
	}
}

func resetComplexCounters() {
	explodeCount.Store(0)
	filterCount.Store(0)
	squareCount.Store(0)
	auditCount.Store(0)
	sumCount.Store(0)
}
