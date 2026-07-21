package duro_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- child option test workflows --------------------------------------------

var (
	priorityQueue  = duro.NewQueue("duro-test-priority", duro.WithConcurrency(2), duro.WithPriorities())
	partitionQueue = duro.NewQueue("duro-test-partition", duro.WithPartitions())
	autoQueue      = duro.NewQueue("duro-test-auto", duro.WithConcurrency(2))
)

func explode(_ context.Context, xs []int) ([]int, error) { return xs, nil }

// childOptsWorkflow combines the identity and metadata child options in one
// FanOut: custom child IDs, auth context, an explicit application version,
// and portable serialization.
func childOptsWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", fanQueue, duro.Workflow(fanChildSquare),
			duro.WithChildID(func(n int) string { return fmt.Sprintf("child-opt-%d", n) }),
			duro.WithChildAuthenticatedUser("alice"),
			duro.WithChildAuthenticatedRoles("ops", "admin"),
			duro.WithChildAssumedRole("ops"),
			duro.WithChildAppVersion(appVersion.Load().(string)),
			duro.WithPortableChildren(),
		),
	))
}

// appVersion holds the application version DBOS computed at launch, captured
// by TestMain so WithChildAppVersion can pass an explicit-but-runnable
// version (children pinned to a foreign version would never dequeue here).
var appVersion atomic.Value

var dedupChildRuns atomic.Int64

// dedupChild sleeps long enough for its sibling to be enqueued while it is
// still active, keeping the deduplication window open deterministically.
func dedupChild(_ dbos.DBOSContext, n int) (int, error) {
	dedupChildRuns.Add(1)
	time.Sleep(300 * time.Millisecond)
	return n * n, nil
}

func dedupFanOutWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", fanQueue, duro.Workflow(dedupChild),
			duro.WithChildDeduplicationID(func(int) string { return "dedup-shared" }),
			duro.WithChildDeduplicationPolicy(duro.DeduplicationReturnExisting),
		),
	))
}

func priorityFanOutWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", priorityQueue, duro.Workflow(fanChildSquare),
			duro.WithChildID(func(n int) string { return fmt.Sprintf("child-prio-%d", n) }),
			duro.WithChildPriority(3),
		),
	))
}

func partitionFanOutWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", partitionQueue, duro.Workflow(fanChildSquare),
			duro.WithChildID(func(n int) string { return fmt.Sprintf("child-part-%d", n) }),
			duro.WithChildPartitionKey(func(n int) string { return fmt.Sprintf("p%d", n%2) }),
		),
	))
}

const childDelayDuration = 1500 * time.Millisecond

func delayFanOutWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", fanQueue, duro.Workflow(fanChildSquare),
			duro.WithChildID(func(n int) string { return fmt.Sprintf("child-delay-%d", n) }),
			duro.WithChildDelay(childDelayDuration),
		),
	))
}

// hangingChild blocks until its durable deadline cancels it.
func hangingChild(ctx dbos.DBOSContext, _ int) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func timeoutFanOutWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explode),
		duro.FanOut("fan", fanQueue, duro.Workflow(hangingChild),
			duro.WithChildID(func(n int) string { return fmt.Sprintf("child-timeout-%d", n) }),
			duro.WithChildTimeout(500*time.Millisecond),
		),
	))
}

// autoQueuePipeline references autoQueue, which no test registers explicitly:
// registering the pipeline must register the queue.
var (
	autoQueueWf     *duro.PipelineWorkflow[[]int, []int]
	squareChildWf   *duro.PipelineWorkflow[int, int]
	pipelineChildWf *duro.PipelineWorkflow[[]int, []int]
)

// collectInts folds a multi-item stream back into a slice, so a registered
// pipeline (which returns the last emitted value) yields every result.
func collectInts() duro.Stage[int, []int] {
	return duro.Reduce("collect", func(_ context.Context, acc []int, v int) ([]int, error) {
		return append(acc, v), nil
	}, nil)
}

var autoQueuePipeline = duro.Pipe3(
	duro.Expand("explode", explode),
	duro.FanOut("fan", autoQueue, duro.Workflow(fanChildSquare)),
	collectInts(),
)

var squareChildPipeline = duro.Pipe1(
	duro.Step("sq", func(_ context.Context, v int) (int, error) { return v * v, nil }),
)

func registerFanOutOptionWorkflows(ctx dbos.DBOSContext) error {
	dbos.RegisterWorkflow(ctx, dedupChild, dbos.WithWorkflowName("dedupChild"))
	dbos.RegisterWorkflow(ctx, hangingChild, dbos.WithWorkflowName("hangingChild"))
	dbos.RegisterWorkflow(ctx, childOptsWorkflow, dbos.WithWorkflowName("childOptsWorkflow"))
	dbos.RegisterWorkflow(ctx, dedupFanOutWorkflow, dbos.WithWorkflowName("dedupFanOutWorkflow"))
	dbos.RegisterWorkflow(ctx, priorityFanOutWorkflow, dbos.WithWorkflowName("priorityFanOutWorkflow"))
	dbos.RegisterWorkflow(ctx, partitionFanOutWorkflow, dbos.WithWorkflowName("partitionFanOutWorkflow"))
	dbos.RegisterWorkflow(ctx, delayFanOutWorkflow, dbos.WithWorkflowName("delayFanOutWorkflow"))
	dbos.RegisterWorkflow(ctx, timeoutFanOutWorkflow, dbos.WithWorkflowName("timeoutFanOutWorkflow"))

	if err := duro.RegisterQueues(ctx, priorityQueue, partitionQueue); err != nil {
		return err
	}

	// autoQueue is deliberately NOT in RegisterQueues: duro.Register must
	// register it because autoQueuePipeline references it.
	autoQueueWf = duro.Register(ctx, "autoQueuePipeline", autoQueuePipeline)
	squareChildWf = duro.Register(ctx, "squareChild", squareChildPipeline)
	pipelineChildWf = duro.Register(ctx, "pipelineChildParent", duro.Pipe3(
		duro.Expand("explode", explode),
		duro.FanOut("fan", fanQueue, squareChildWf),
		collectInts(),
	))
	return nil
}

// childStatus fetches a child workflow's status by its deterministic ID.
func childStatus(t *testing.T, childID string) dbos.WorkflowStatus {
	t.Helper()
	handle, err := dbos.RetrieveWorkflow[int](dctx, childID)
	if err != nil {
		t.Fatalf("retrieving child %s: %v", childID, err)
	}
	status, err := handle.GetStatus()
	if err != nil {
		t.Fatalf("status of child %s: %v", childID, err)
	}
	return status
}

func assertInts(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result = %v, want %v", got, want)
		}
	}
}

// --- tests ------------------------------------------------------------------

// TestFanOutChildIdentityAndMetadata proves WithChildID, the auth options,
// WithChildAppVersion, and WithPortableChildren all reach the children: each
// child is addressable by its derived ID and its status carries the metadata.
func TestFanOutChildIdentityAndMetadata(t *testing.T) {
	result, _ := mustRun(t, childOptsWorkflow, []int{2, 3})
	assertInts(t, result, []int{4, 9})

	for _, n := range []int{2, 3} {
		status := childStatus(t, fmt.Sprintf("child-opt-%d", n))
		if status.Status != dbos.WorkflowStatusSuccess {
			t.Errorf("child %d status = %s, want SUCCESS", n, status.Status)
		}
		if status.AuthenticatedUser != "alice" {
			t.Errorf("child %d authenticated user = %q, want %q", n, status.AuthenticatedUser, "alice")
		}
		if len(status.AuthenticatedRoles) != 2 || status.AuthenticatedRoles[0] != "ops" || status.AuthenticatedRoles[1] != "admin" {
			t.Errorf("child %d authenticated roles = %v, want [ops admin]", n, status.AuthenticatedRoles)
		}
		if status.AssumedRole != "ops" {
			t.Errorf("child %d assumed role = %q, want %q", n, status.AssumedRole, "ops")
		}
		if want := appVersion.Load().(string); status.ApplicationVersion != want {
			t.Errorf("child %d application version = %q, want %q", n, status.ApplicationVersion, want)
		}
		if !strings.Contains(strings.ToLower(status.Serialization), "portable") {
			t.Errorf("child %d serialization = %q, want portable", n, status.Serialization)
		}
	}
}

// TestFanOutChildDeduplication proves a shared deduplication ID with
// ReturnExisting collapses siblings onto one child: both items get the first
// child's result and the child body runs once.
func TestFanOutChildDeduplication(t *testing.T) {
	dedupChildRuns.Store(0)

	result, _ := mustRun(t, dedupFanOutWorkflow, []int{7, 9})
	// Item 9's enqueue returns the existing child (7²), so both emit 49.
	assertInts(t, result, []int{49, 49})
	if got := dedupChildRuns.Load(); got != 1 {
		t.Errorf("child executions = %d, want 1 (duplicate collapsed)", got)
	}
}

// TestFanOutChildPriority proves the priority reaches enqueued children on a
// priority-enabled queue.
func TestFanOutChildPriority(t *testing.T) {
	result, _ := mustRun(t, priorityFanOutWorkflow, []int{4, 5})
	assertInts(t, result, []int{16, 25})

	for _, n := range []int{4, 5} {
		if got := childStatus(t, fmt.Sprintf("child-prio-%d", n)).Priority; got != 3 {
			t.Errorf("child %d priority = %d, want 3", n, got)
		}
	}
}

// TestFanOutChildPartitionKey proves per-item partition keys route children
// onto queue partitions.
func TestFanOutChildPartitionKey(t *testing.T) {
	result, _ := mustRun(t, partitionFanOutWorkflow, []int{1, 2, 3, 4})
	assertInts(t, result, []int{1, 4, 9, 16})

	for _, n := range []int{1, 2, 3, 4} {
		want := fmt.Sprintf("p%d", n%2)
		if got := childStatus(t, fmt.Sprintf("child-part-%d", n)).QueuePartitionKey; got != want {
			t.Errorf("child %d partition key = %q, want %q", n, got, want)
		}
	}
}

// TestFanOutChildDelay proves WithChildDelay holds children in DELAYED status
// with a recorded wake-up time before they run.
func TestFanOutChildDelay(t *testing.T) {
	start := time.Now()
	handle, err := dbos.RunWorkflow(dctx, delayFanOutWorkflow, []int{6})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}

	// The child is enqueued DELAYED almost immediately; observe it before the
	// delay lapses.
	const childID = "child-delay-6"
	var status dbos.WorkflowStatus
	for {
		h, err := dbos.RetrieveWorkflow[int](dctx, childID)
		if err == nil {
			if status, err = h.GetStatus(); err == nil {
				break
			}
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("child %s not found: %v", childID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status.Status != dbos.WorkflowStatusDelayed {
		t.Errorf("child status = %s, want DELAYED", status.Status)
	}
	if status.DelayUntil.IsZero() {
		t.Errorf("child DelayUntil is zero, want the recorded wake-up time")
	}

	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	assertInts(t, result, []int{36})
	if elapsed := time.Since(start); elapsed < childDelayDuration {
		t.Errorf("workflow finished in %v, want ≥ %v (delay not applied)", elapsed, childDelayDuration)
	}
}

// TestFanOutChildTimeout proves WithChildTimeout gives children a durable
// deadline that cancels them and fails the pipeline.
func TestFanOutChildTimeout(t *testing.T) {
	start := time.Now()
	handle, err := dbos.RunWorkflow(dctx, timeoutFanOutWorkflow, []int{1})
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), `stage "fan"`) {
		t.Fatalf("workflow error = %v, want the fan stage's timed-out child failure", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Errorf("timed-out child took %v to fail the pipeline", elapsed)
	}

	if deadline := childStatus(t, "child-timeout-1").Deadline; deadline.IsZero() {
		t.Errorf("child deadline is zero, want the durable timeout deadline")
	}
}

// TestFanOutOptionTypeMismatchPanics proves the construction-time guard for
// item-derived options built for the wrong item type.
func TestFanOutOptionTypeMismatchPanics(t *testing.T) {
	assertPanics(t, "mismatched child option type", func() {
		duro.FanOut("fan", fanQueue, duro.Workflow(fanChildSquare),
			duro.WithChildID(func(s string) string { return s }), // stage item type is int
		)
	})
}

// TestFanOutAutoRegistersQueues proves Register registered autoQueue because
// the pipeline references it — no RegisterQueues call anywhere.
func TestFanOutAutoRegistersQueues(t *testing.T) {
	handle, err := autoQueueWf.Start(app, []int{2, 3, 4})
	if err != nil {
		t.Fatalf("starting pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	assertInts(t, result, []int{4, 9, 16})
}

// TestFanOutIntoPipeline proves a registered pipeline is passable directly as
// a FanOut child: the WorkflowRef carries its instance dispatch, no extra
// option needed.
func TestFanOutIntoPipeline(t *testing.T) {
	handle, err := pipelineChildWf.Start(app, []int{5, 6})
	if err != nil {
		t.Fatalf("starting parent pipeline: %v", err)
	}
	result, err := handle.Result()
	if err != nil {
		t.Fatalf("parent pipeline failed: %v", err)
	}
	assertInts(t, result, []int{25, 36})
}

// TestConflictingQueueDeclarationsFail proves two same-named queue
// declarations with different configurations are rejected instead of
// silently overwriting each other.
func TestConflictingQueueDeclarationsFail(t *testing.T) {
	q := duro.NewQueue("duro-test-conflict", duro.WithConcurrency(2))
	if err := duro.RegisterQueues(app, q); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	// Same declaration again: no-op.
	if err := duro.RegisterQueues(app, q); err != nil {
		t.Fatalf("re-registration of the same declaration: %v", err)
	}
	// Same name, different config: loud failure.
	clash := duro.NewQueue("duro-test-conflict", duro.WithConcurrency(3))
	err := duro.RegisterQueues(app, clash)
	if err == nil || !strings.Contains(err.Error(), "declared twice") {
		t.Errorf("error = %v, want the conflicting-declaration error", err)
	}
}
