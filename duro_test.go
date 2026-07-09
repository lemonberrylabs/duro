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
	dbos.RegisterWorkflow(ctx, fanChildSquare, dbos.WithWorkflowName("fanChildSquare"))
	dbos.RegisterWorkflow(ctx, fanOutWorkflow, dbos.WithWorkflowName("fanOutWorkflow"))
	dbos.RegisterWorkflow(ctx, fanOutAllWorkflow, dbos.WithWorkflowName("fanOutAllWorkflow"))
	dbos.RegisterWorkflow(ctx, parallelWorkflow, dbos.WithWorkflowName("parallelWorkflow"))
	dbos.RegisterWorkflow(ctx, parallelAllWorkflow, dbos.WithWorkflowName("parallelAllWorkflow"))
	dbos.RegisterWorkflow(ctx, delayWorkflow, dbos.WithWorkflowName("delayWorkflow"))
	dbos.RegisterWorkflow(ctx, recvGreetingWorkflow, dbos.WithWorkflowName("recvGreetingWorkflow"))
	dbos.RegisterWorkflow(ctx, progressWorkflow, dbos.WithWorkflowName("progressWorkflow"))
	dbos.RegisterWorkflow(ctx, streamingWorkflow, dbos.WithWorkflowName("streamingWorkflow"))
	dbos.RegisterWorkflow(ctx, senderWorkflow, dbos.WithWorkflowName("senderWorkflow"))
	dbos.RegisterWorkflow(ctx, pipe7Workflow, dbos.WithWorkflowName("pipe7Workflow"))
	dbos.RegisterWorkflow(ctx, pipe8Workflow, dbos.WithWorkflowName("pipe8Workflow"))

	if _, err := dbos.RegisterQueue(ctx, fanQueueName, dbos.WithGlobalConcurrency(fanQueueConcurrency)); err != nil {
		fmt.Fprintf(os.Stderr, "registering queue: %v\n", err)
		os.Exit(1)
	}

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

// --- fan-out test workflows --------------------------------------------------

const (
	fanQueueName        = "duro-test-fanout"
	fanQueueConcurrency = 4
)

var (
	fanChildRuns          atomic.Int64
	fanChildActive        atomic.Int64
	fanChildMaxConcurrent atomic.Int64
)

// fanChildSquare is the child workflow FanOut spawns per item. It tracks how
// many instances run concurrently so tests can assert the queue's concurrency
// cap, and sleeps long enough for executions to overlap.
func fanChildSquare(_ dbos.DBOSContext, n int) (int, error) {
	fanChildRuns.Add(1)
	active := fanChildActive.Add(1)
	defer fanChildActive.Add(-1)
	for {
		seen := fanChildMaxConcurrent.Load()
		if active <= seen || fanChildMaxConcurrent.CompareAndSwap(seen, active) {
			break
		}
	}
	time.Sleep(40 * time.Millisecond)
	if n < 0 {
		return 0, fmt.Errorf("cannot square a grumpy number: %d", n)
	}
	return n * n, nil
}

func fanOutWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.FanOut("fan", fanQueueName, fanChildSquare),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

func fanOutAllWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.FanOut("fan", fanQueueName, fanChildSquare),
	))
}

func resetFanCounters() {
	fanChildRuns.Store(0)
	fanChildActive.Store(0)
	fanChildMaxConcurrent.Store(0)
}

// --- parallel / signal / stream test workflows -------------------------------

var (
	parRuns          atomic.Int64
	parActive        atomic.Int64
	parMaxConcurrent atomic.Int64

	delayPreRuns  atomic.Int64
	delayPostRuns atomic.Int64
)

func trackedParSquare(_ context.Context, n int) (int, error) {
	parRuns.Add(1)
	active := parActive.Add(1)
	defer parActive.Add(-1)
	for {
		seen := parMaxConcurrent.Load()
		if active <= seen || parMaxConcurrent.CompareAndSwap(seen, active) {
			break
		}
	}
	time.Sleep(40 * time.Millisecond)
	if n < 0 {
		return 0, fmt.Errorf("cannot square a grumpy number: %d", n)
	}
	return n * n, nil
}

func parallelWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.Parallel("par", 4, trackedParSquare),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

func parallelAllWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.Parallel("par", 4, trackedParSquare),
	))
}

const delayDuration = 2 * time.Second

func delayWorkflow(ctx dbos.DBOSContext, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe3(
		duro.Step("pre", func(_ context.Context, v int) (int, error) {
			delayPreRuns.Add(1)
			return v + 1, nil
		}),
		duro.Delay[int]("pause", delayDuration),
		duro.Step("post", func(_ context.Context, v int) (int, error) {
			delayPostRuns.Add(1)
			return v * 10, nil
		}),
	))
}

func recvGreetingWorkflow(ctx dbos.DBOSContext, _ string) (string, error) {
	return duro.Run(ctx, "", duro.Pipe2(
		duro.Recv[string, string]("await-note", "notes", 10*time.Second),
		duro.Step("decorate", func(_ context.Context, note string) (string, error) {
			return "received: " + note, nil
		}),
	))
}

func progressWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe5(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.Pure("as-is", func(v int) int { return v }),
		duro.SetEvent("progress", "last-item", func(v int) int { return v }),
		duro.Pure("still-as-is", func(v int) int { return v }),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

// senderWorkflow notifies another workflow's mailbox through a duro.Send
// stage, then returns its input decorated.
func senderWorkflow(ctx dbos.DBOSContext, destinationID string) (string, error) {
	return duro.Run(ctx, destinationID, duro.Pipe2(
		duro.Send("notify", "notes", func(dest string) (string, string, error) {
			return dest, "ping from " + dest, nil
		}),
		duro.Step("done", func(_ context.Context, dest string) (string, error) {
			return "notified " + dest, nil
		}),
	))
}

// pipe7Workflow and pipe8Workflow are smoke coverage for the wide pipe
// arities: linear int pipelines mixing durable and pure stages.
func pipe7Workflow(ctx dbos.DBOSContext, n int) (int, error) {
	inc := func(_ context.Context, v int) (int, error) { return v + 1, nil }
	return duro.Run(ctx, n, duro.Pipe7(
		duro.Step("s1", inc),
		duro.Pure("p1", func(v int) int { return v }),
		duro.Step("s2", inc),
		duro.Tap("t1", func(_ context.Context, _ int) error { return nil }),
		duro.Step("s3", inc),
		duro.Pure("p2", func(v int) int { return v }),
		duro.Step("s4", inc),
	))
}

func pipe8Workflow(ctx dbos.DBOSContext, n int) (int, error) {
	inc := func(_ context.Context, v int) (int, error) { return v + 1, nil }
	return duro.Run(ctx, n, duro.Pipe8(
		duro.Step("s1", inc),
		duro.Pure("p1", func(v int) int { return v }),
		duro.Step("s2", inc),
		duro.Filter("keep-all", func(_ context.Context, _ int) (bool, error) { return true, nil }),
		duro.Step("s3", inc),
		duro.Pure("p2", func(v int) int { return v }),
		duro.Step("s4", inc),
		duro.Step("s5", inc),
	))
}

func streamingWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, xs []int) ([]int, error) {
			return xs, nil
		}),
		duro.ToStream[int]("emit", "out"),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

func resetParCounters() {
	parRuns.Store(0)
	parActive.Store(0)
	parMaxConcurrent.Store(0)
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
	assertPanics(t, "empty FanOut queue", func() {
		duro.FanOut("fan", "", fanChildSquare)
	})
	assertPanics(t, "nil Parallel function", func() {
		duro.Parallel[int, int]("par", 4, nil)
	})
	assertPanics(t, "empty Delay name", func() {
		duro.Delay[int]("", time.Second)
	})
	assertPanics(t, "non-positive Delay duration", func() {
		duro.Delay[int]("pause", 0)
	})
	assertPanics(t, "empty Recv name", func() {
		duro.Recv[int, string]("", "topic", time.Second)
	})
	assertPanics(t, "empty SetEvent key", func() {
		duro.SetEvent("progress", "", func(v int) int { return v })
	})
	assertPanics(t, "empty ToStream key", func() {
		duro.ToStream[int]("emit", "")
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

// TestFanOutParallelMapReduce proves the headline pattern: 20 jobs fanned out
// as child workflows on a queue capped at 4 concurrent, results merged by a
// durable Reduce. The concurrency cap must hold and real overlap must occur.
func TestFanOutParallelMapReduce(t *testing.T) {
	resetFanCounters()

	jobs := make([]int, 20)
	wantSum := 0
	for i := range jobs {
		jobs[i] = i + 1
		wantSum += (i + 1) * (i + 1)
	}

	result, _ := mustRun(t, fanOutWorkflow, jobs)
	if result != wantSum {
		t.Errorf("result = %d, want %d", result, wantSum)
	}
	if got := fanChildRuns.Load(); got != 20 {
		t.Errorf("child executions = %d, want 20", got)
	}
	maxSeen := fanChildMaxConcurrent.Load()
	if maxSeen > fanQueueConcurrency {
		t.Errorf("max concurrent children = %d, must not exceed the queue cap %d", maxSeen, fanQueueConcurrency)
	}
	if maxSeen < 2 {
		t.Errorf("max concurrent children = %d, want ≥ 2 (no parallelism happened)", maxSeen)
	}
}

// TestFanOutPreservesOrder proves results are emitted in input order (not
// completion order) and shows the checkpoint layout: one child-spawn step per
// item at enqueue time, then one DBOS.getResult step per awaited child.
func TestFanOutPreservesOrder(t *testing.T) {
	resetFanCounters()

	result, wfID := mustRun(t, fanOutAllWorkflow, []int{3, 1, 2})
	want := []int{9, 1, 4}
	if len(result) != len(want) {
		t.Fatalf("result = %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("result = %v, want %v (input order must be preserved)", result, want)
		}
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName,
		"explode",
		"fanChildSquare", "fanChildSquare", "fanChildSquare", // enqueue slots
		"DBOS.getResult", "DBOS.getResult", "DBOS.getResult", // awaited in order
	})
}

// TestFanOutDurabilityRerun proves re-running a completed fan-out workflow ID
// replays everything: no child workflow executes again.
func TestFanOutDurabilityRerun(t *testing.T) {
	resetFanCounters()
	const wfID = "fanout-durability-rerun"

	first, _ := mustRun(t, fanOutWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	runsAfterFirst := fanChildRuns.Load()

	second, _ := mustRun(t, fanOutWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	if first != second {
		t.Errorf("rerun result = %d, want recorded result %d", second, first)
	}
	if got := fanChildRuns.Load(); got != runsAfterFirst {
		t.Errorf("child executions changed on rerun: %d → %d (children re-executed instead of replayed)", runsAfterFirst, got)
	}
}

// TestFanOutChildFailure proves a failing child fails the parent pipeline
// with an error identifying the stage and child workflow.
func TestFanOutChildFailure(t *testing.T) {
	resetFanCounters()

	handle, err := dbos.RunWorkflow(dctx, fanOutWorkflow, []int{1, -2, 3})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), `stage "fan"`) || !strings.Contains(err.Error(), "grumpy") {
		t.Fatalf("workflow error = %v, want the fan stage's child failure", err)
	}
}

// TestParallelBoundedMapReduce proves in-process bounded parallelism: 20
// items squared by concurrent DBOS steps (dbos.Go under the hood), at most 4
// at a time, merged by a durable Reduce.
func TestParallelBoundedMapReduce(t *testing.T) {
	resetParCounters()

	jobs := make([]int, 20)
	wantSum := 0
	for i := range jobs {
		jobs[i] = i + 1
		wantSum += (i + 1) * (i + 1)
	}

	result, _ := mustRun(t, parallelWorkflow, jobs)
	if result != wantSum {
		t.Errorf("result = %d, want %d", result, wantSum)
	}
	if got := parRuns.Load(); got != 20 {
		t.Errorf("step executions = %d, want 20", got)
	}
	maxSeen := parMaxConcurrent.Load()
	if maxSeen > 4 {
		t.Errorf("max concurrent steps = %d, must not exceed 4", maxSeen)
	}
	if maxSeen < 2 {
		t.Errorf("max concurrent steps = %d, want ≥ 2 (no parallelism happened)", maxSeen)
	}
}

// TestParallelPreservesOrder proves results are emitted in input order and
// that each item occupies one pre-assigned step slot, in stream order.
func TestParallelPreservesOrder(t *testing.T) {
	resetParCounters()

	result, wfID := mustRun(t, parallelAllWorkflow, []int{3, 1, 2})
	want := []int{9, 1, 4}
	if len(result) != len(want) {
		t.Fatalf("result = %v, want %v", result, want)
	}
	for i := range want {
		if result[i] != want[i] {
			t.Fatalf("result = %v, want %v (input order must be preserved)", result, want)
		}
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName, "explode", "par", "par", "par",
	})
}

// TestParallelDurabilityRerun proves a completed parallel workflow replays
// with zero step re-executions.
func TestParallelDurabilityRerun(t *testing.T) {
	resetParCounters()
	const wfID = "parallel-durability-rerun"

	first, _ := mustRun(t, parallelWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	runsAfterFirst := parRuns.Load()

	second, _ := mustRun(t, parallelWorkflow, []int{1, 2, 3, 4, 5}, dbos.WithWorkflowID(wfID))
	if first != second {
		t.Errorf("rerun result = %d, want recorded result %d", second, first)
	}
	if got := parRuns.Load(); got != runsAfterFirst {
		t.Errorf("step executions changed on rerun: %d → %d", runsAfterFirst, got)
	}
}

func TestParallelStepFailure(t *testing.T) {
	resetParCounters()

	handle, err := dbos.RunWorkflow(dctx, parallelWorkflow, []int{1, -2, 3})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), `stage "par"`) || !strings.Contains(err.Error(), "grumpy") {
		t.Fatalf("workflow error = %v, want the par stage's step failure", err)
	}
}

// TestDelayDurableSleep proves Delay really pauses, and — via a fork replay
// past the sleep's step slot — that a recovered workflow does not sleep again.
func TestDelayDurableSleep(t *testing.T) {
	delayPreRuns.Store(0)
	delayPostRuns.Store(0)

	start := time.Now()
	result, wfID := mustRun(t, delayWorkflow, 4)
	elapsed := time.Since(start)
	if result != 50 {
		t.Errorf("result = %d, want 50", result)
	}
	if elapsed < delayDuration {
		t.Errorf("first run took %v, want ≥ %v (Delay did not pause)", elapsed, delayDuration)
	}

	// Fork past the sleep slot (0 shape, 1 pre, 2 sleep, 3 post): the sleep
	// replays from its checkpoint, so the fork must complete far faster than
	// the sleep duration while re-executing only the post step.
	start = time.Now()
	handle, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: wfID,
		StartStep:          3,
	})
	if err != nil {
		t.Fatalf("forking workflow: %v", err)
	}
	forked, err := handle.GetResult()
	if err != nil {
		t.Fatalf("forked workflow failed: %v", err)
	}
	forkElapsed := time.Since(start)
	if forked != result {
		t.Errorf("forked result = %d, want %d", forked, result)
	}
	if forkElapsed >= delayDuration {
		t.Errorf("fork replay took %v, want < %v (sleep re-executed instead of replayed)", forkElapsed, delayDuration)
	}
	if pre, post := delayPreRuns.Load(), delayPostRuns.Load(); pre != 1 || post != 2 {
		t.Errorf("pre/post executions = %d/%d, want 1/2 (fork replays pre and sleep, re-executes post)", pre, post)
	}
}

// TestSendRecvBridge proves a pipeline can durably pause for an external
// signal: the workflow blocks in Recv until a message is sent to its mailbox.
func TestSendRecvBridge(t *testing.T) {
	const wfID = "recv-greeting"

	handle, err := dbos.RunWorkflow(dctx, recvGreetingWorkflow, "", dbos.WithWorkflowID(wfID))
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	if err := dbos.Send(dctx, wfID, "hello duro", "notes"); err != nil {
		t.Fatalf("sending message: %v", err)
	}
	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	if result != "received: hello duro" {
		t.Errorf("result = %q, want %q", result, "received: hello duro")
	}
}

// TestSendStageBridgesWorkflows is the workflow-to-workflow variant: a
// duro.Send stage in one pipeline unblocks a duro.Recv stage in another.
func TestSendStageBridgesWorkflows(t *testing.T) {
	const receiverID = "recv-greeting-from-stage"

	receiver, err := dbos.RunWorkflow(dctx, recvGreetingWorkflow, "", dbos.WithWorkflowID(receiverID))
	if err != nil {
		t.Fatalf("starting receiver: %v", err)
	}
	senderResult, _ := mustRun(t, senderWorkflow, receiverID)
	if senderResult != "notified "+receiverID {
		t.Errorf("sender result = %q", senderResult)
	}
	received, err := receiver.GetResult()
	if err != nil {
		t.Fatalf("receiver failed: %v", err)
	}
	if received != "received: ping from "+receiverID {
		t.Errorf("receiver result = %q", received)
	}
}

// TestWidePipes is smoke coverage for the Pipe7/Pipe8 arities.
func TestWidePipes(t *testing.T) {
	if got, _ := mustRun(t, pipe7Workflow, 0); got != 4 {
		t.Errorf("pipe7 result = %d, want 4", got)
	}
	if got, _ := mustRun(t, pipe8Workflow, 0); got != 5 {
		t.Errorf("pipe8 result = %d, want 5", got)
	}
}

// TestSetEventExposesProgress proves SetEvent publishes per-item progress
// readable from outside the workflow via dbos.GetEvent.
func TestSetEventExposesProgress(t *testing.T) {
	result, wfID := mustRun(t, progressWorkflow, []int{1, 2, 3})
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}
	last, err := dbos.GetEvent[int](dctx, wfID, "last-item", 5*time.Second)
	if err != nil {
		t.Fatalf("reading event: %v", err)
	}
	if last != 3 {
		t.Errorf("last-item event = %d, want 3 (the final item)", last)
	}
}

// TestToStreamPublishesItems proves ToStream writes every item to a durable
// stream, closed at pipeline completion, readable via dbos.ReadStream.
func TestToStreamPublishesItems(t *testing.T) {
	result, wfID := mustRun(t, streamingWorkflow, []int{1, 2, 3})
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}
	values, closed, err := dbos.ReadStream[int](dctx, wfID, "out")
	if err != nil {
		t.Fatalf("reading stream: %v", err)
	}
	if !closed {
		t.Errorf("stream not closed after pipeline completion")
	}
	want := []int{1, 2, 3}
	if len(values) != len(want) {
		t.Fatalf("stream values = %v, want %v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("stream values = %v, want %v", values, want)
		}
	}
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
