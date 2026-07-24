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

// Sibling-cancellation tests (WithCancelSiblings / WithCancelSiblingSteps).
//
// Each test uses a disjoint range of item values, so the child workflow IDs
// derived by cancelChildID never collide across tests sharing the queues, and
// per-test counter deltas stay attributable.

var (
	cancelWideQueue = duro.NewQueue("cancel-wide")
	// One start per five seconds: after the first (failing) child starts, no
	// sibling can be dequeued inside a test's time budget — which makes
	// "ENQUEUED children are cancelled without ever running" deterministic
	// instead of a race against the queue runner.
	cancelLimitedQueue = duro.NewQueue("cancel-limited", duro.WithConcurrency(1), duro.WithRateLimit(1, 5*time.Second))

	cancelChildStarted   atomic.Int64
	cancelChildCompleted atomic.Int64
	cancelShortDone      atomic.Int64
	cancelGrandchildDone atomic.Int64

	fanRescueCause atomic.Value // string

	parCancelObserved atomic.Int64
	parSkipRuns       atomic.Int64
	parRescueRuns     atomic.Int64
	parRescueCause    atomic.Value // string

	fanCancelToggle atomic.Bool
)

// cancelChildTicks × 100ms is how long a blocking child runs if nothing
// cancels it — the ceiling every promptness assertion is measured against.
const cancelChildTicks = 60

func cancelChildID(n int) string { return fmt.Sprintf("cancel-child-%d", n) }

// cancelMixChild fails shortly after starting for negative items; otherwise
// it blocks through many short steps, so DBOS cancellation (detected at step
// boundaries) can end it quickly while natural completion takes far longer
// than any test budget.
func cancelMixChild(ctx dbos.DBOSContext, n int) (int, error) {
	cancelChildStarted.Add(1)
	if n < 0 {
		time.Sleep(100 * time.Millisecond)
		return 0, fmt.Errorf("synthetic child failure %d", n)
	}
	for i := range cancelChildTicks {
		if _, err := dbos.RunAsStep(ctx, func(context.Context) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return i, nil
		}, dbos.WithStepName("tick")); err != nil {
			return 0, err
		}
	}
	cancelChildCompleted.Add(1)
	return n * n, nil
}

// cancelShortChild completes (or fails) quickly — for drain-semantics and
// happy-path tests where children finishing on their own is the point.
func cancelShortChild(_ dbos.DBOSContext, n int) (int, error) {
	if n < 0 {
		time.Sleep(50 * time.Millisecond)
		return 0, fmt.Errorf("synthetic child failure %d", n)
	}
	time.Sleep(150 * time.Millisecond)
	cancelShortDone.Add(1)
	return n * n, nil
}

func cancelGrandchild(_ dbos.DBOSContext, n int) (int, error) {
	time.Sleep(4 * time.Second)
	cancelGrandchildDone.Add(1)
	return n, nil
}

// cancelNestedParent fails slowly for negative items; otherwise it runs its
// own drain-mode fan-out of one grandchild and awaits it — the shape that
// proves cancellation does not cascade.
func cancelNestedParent(ctx dbos.DBOSContext, n int) (int, error) {
	if n < 0 {
		time.Sleep(1 * time.Second)
		return 0, fmt.Errorf("synthetic child failure %d", n)
	}
	return duro.Run(ctx, n, duro.Pipe1(
		duro.FanOut("inner", cancelWideQueue, duro.Workflow(cancelGrandchild)),
	))
}

func explodeInts(_ context.Context, xs []int) ([]int, error) { return xs, nil }

func fanCancelWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("fan", cancelWideQueue, duro.Workflow(cancelMixChild),
			duro.WithCancelSiblings(), duro.WithChildID(cancelChildID),
			duro.WithCancelWatchInterval(500*time.Millisecond)),
	))
}

func fanCancelLimitedWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("fan", cancelLimitedQueue, duro.Workflow(cancelMixChild),
			duro.WithCancelSiblings(), duro.WithChildID(cancelChildID)),
	))
}

func fanCancelShortWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("fan", cancelWideQueue, duro.Workflow(cancelShortChild),
			duro.WithCancelSiblings(), duro.WithChildID(cancelChildID),
			duro.WithCancelWatchInterval(500*time.Millisecond)),
	))
}

func fanDrainWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("fan", cancelWideQueue, duro.Workflow(cancelShortChild),
			duro.WithChildID(cancelChildID)),
	))
}

func fanCancelRescueWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe1(
		duro.Rescue("save", duro.Pipe2(
			duro.Expand("explode", explodeInts),
			duro.FanOut("fan", cancelWideQueue, duro.Workflow(cancelMixChild),
				duro.WithCancelSiblings(), duro.WithChildID(cancelChildID)),
		), func(_ context.Context, _ []int, cause error) (int, error) {
			fanRescueCause.Store(cause.Error())
			return -1, nil
		}),
	))
}

func fanCancelNestedWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("outer", cancelWideQueue, duro.Workflow(cancelNestedParent),
			duro.WithCancelSiblings(), duro.WithChildID(cancelChildID)),
	))
}

// fanShapeToggleWorkflow flips WithCancelSiblings on a package flag —
// simulating a deploy that toggles the option while a run is in flight.
func fanShapeToggleWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	opts := []duro.ChildOption{duro.WithChildID(cancelChildID)}
	if fanCancelToggle.Load() {
		opts = append(opts, duro.WithCancelSiblings())
	}
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.FanOut("fan", cancelWideQueue, duro.Workflow(cancelShortChild), opts...),
	))
}

func parCancelWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.Parallel("par", 0, func(ctx context.Context, n int) (int, error) {
			if n < 0 {
				time.Sleep(100 * time.Millisecond)
				return 0, fmt.Errorf("synthetic step failure %d", n)
			}
			select {
			case <-ctx.Done():
				parCancelObserved.Add(1)
				return 0, ctx.Err()
			case <-time.After(5 * time.Second):
				return n * n, nil
			}
		}, duro.WithCancelSiblingSteps()),
	))
}

func parSkipWorkflow(ctx dbos.DBOSContext, ns []int) ([]int, error) {
	return duro.RunAll(ctx, ns, duro.Pipe2(
		duro.Expand("explode", explodeInts),
		duro.Parallel("par", 1, func(_ context.Context, n int) (int, error) {
			parSkipRuns.Add(1)
			if n < 0 {
				return 0, fmt.Errorf("synthetic step failure %d", n)
			}
			return n * n, nil
		}, duro.WithCancelSiblingSteps()),
	))
}

func parCancelRescueWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe1(
		duro.Rescue("save", duro.Pipe2(
			duro.Expand("explode", explodeInts),
			duro.Parallel("par", 0, func(ctx context.Context, n int) (int, error) {
				parRescueRuns.Add(1)
				if n < 0 {
					time.Sleep(100 * time.Millisecond)
					return 0, fmt.Errorf("synthetic step failure %d", n)
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(5 * time.Second):
					return n * n, nil
				}
			}, duro.WithCancelSiblingSteps()),
		), func(_ context.Context, _ []int, cause error) (int, error) {
			parRescueCause.Store(cause.Error())
			return -1, nil
		}),
	))
}

func registerCancelWorkflows(ctx dbos.DBOSContext) {
	dbos.RegisterWorkflow(ctx, cancelMixChild, dbos.WithWorkflowName("cancelMixChild"))
	dbos.RegisterWorkflow(ctx, cancelShortChild, dbos.WithWorkflowName("cancelShortChild"))
	dbos.RegisterWorkflow(ctx, cancelGrandchild, dbos.WithWorkflowName("cancelGrandchild"))
	dbos.RegisterWorkflow(ctx, cancelNestedParent, dbos.WithWorkflowName("cancelNestedParent"))
	dbos.RegisterWorkflow(ctx, fanCancelWorkflow, dbos.WithWorkflowName("fanCancelWorkflow"))
	dbos.RegisterWorkflow(ctx, fanCancelLimitedWorkflow, dbos.WithWorkflowName("fanCancelLimitedWorkflow"))
	dbos.RegisterWorkflow(ctx, fanCancelShortWorkflow, dbos.WithWorkflowName("fanCancelShortWorkflow"))
	dbos.RegisterWorkflow(ctx, fanDrainWorkflow, dbos.WithWorkflowName("fanDrainWorkflow"))
	dbos.RegisterWorkflow(ctx, fanCancelRescueWorkflow, dbos.WithWorkflowName("fanCancelRescueWorkflow"))
	dbos.RegisterWorkflow(ctx, fanCancelNestedWorkflow, dbos.WithWorkflowName("fanCancelNestedWorkflow"))
	dbos.RegisterWorkflow(ctx, fanShapeToggleWorkflow, dbos.WithWorkflowName("fanShapeToggleWorkflow"))
	dbos.RegisterWorkflow(ctx, parCancelWorkflow, dbos.WithWorkflowName("parCancelWorkflow"))
	dbos.RegisterWorkflow(ctx, parSkipWorkflow, dbos.WithWorkflowName("parSkipWorkflow"))
	dbos.RegisterWorkflow(ctx, parCancelRescueWorkflow, dbos.WithWorkflowName("parCancelRescueWorkflow"))
}

// --- helpers ---------------------------------------------------------------

func mustFail[P, R any](t *testing.T, wf dbos.Workflow[P, R], input P) (error, string) {
	t.Helper()
	handle, err := dbos.RunWorkflow(dctx, wf, input)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil {
		t.Fatalf("workflow succeeded, want a failure")
	}
	return err, handle.GetWorkflowID()
}

func childState(t *testing.T, workflowID string) dbos.WorkflowStatusType {
	t.Helper()
	statuses, err := dbos.ListWorkflows(dctx,
		dbos.WithWorkflowIDs([]string{workflowID}),
		dbos.WithLoadInput(false),
		dbos.WithLoadOutput(false),
	)
	if err != nil {
		t.Fatalf("listing workflow %s: %v", workflowID, err)
	}
	if len(statuses) != 1 {
		t.Fatalf("workflow %s not found", workflowID)
	}
	return statuses[0].Status
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// cancelWatcherID resolves the cancellation watcher a cancel-enabled fan-out
// parent enqueued, from the parent's recorded child-spawn step.
func cancelWatcherID(t *testing.T, parentID string) string {
	t.Helper()
	steps, err := dbos.GetWorkflowSteps(dctx, parentID)
	if err != nil {
		t.Fatalf("fetching steps for %s: %v", parentID, err)
	}
	for _, s := range steps {
		if s.StepName == "duro.cancel-watcher" {
			return s.ChildWorkflowID
		}
	}
	t.Fatalf("no cancellation watcher recorded for %s", parentID)
	return ""
}

func stepIndex(t *testing.T, names []string, name string) uint {
	t.Helper()
	for i, n := range names {
		if n == name {
			return uint(i)
		}
	}
	t.Fatalf("step %q not found in %v", name, names)
	return 0
}

// --- FanOut ----------------------------------------------------------------

// TestFanOutCancelSuccessPreservesOrder proves the cancel-enabled happy path:
// results in input order, and the checkpoint layout of one enqueue slot per
// child followed by a single await step named after the stage (instead of one
// DBOS.getResult per child).
func TestFanOutCancelSuccessPreservesOrder(t *testing.T) {
	result, wfID := mustRun(t, fanCancelShortWorkflow, []int{941, 943, 942})
	want := []int{941 * 941, 943 * 943, 942 * 942}
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
		"cancelShortChild", "cancelShortChild", "cancelShortChild",
		"duro.cancel-watcher",
		"fan",
	})
	watcherID := cancelWatcherID(t, wfID)
	eventually(t, 5*time.Second, "the cancellation watcher to exit once all children are terminal", func() bool {
		return childState(t, watcherID) == dbos.WorkflowStatusSuccess
	})
}

// TestFanOutCancelEmptyStream proves an empty fan-out completes without
// recording an await step.
func TestFanOutCancelEmptyStream(t *testing.T) {
	result, wfID := mustRun(t, fanCancelShortWorkflow, []int{})
	if len(result) != 0 {
		t.Fatalf("result = %v, want empty", result)
	}
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "explode"})
}

// TestFanOutCancelSiblingsNeverDequeued proves ENQUEUED children that were
// never dequeued are cancelled: the rate-limited queue lets exactly one child
// (the failing one) start, and the stage must fail long before the limiter
// would release a sibling.
func TestFanOutCancelSiblingsNeverDequeued(t *testing.T) {
	startedBefore := cancelChildStarted.Load()

	start := time.Now()
	err, wfID := mustFail(t, fanCancelLimitedWorkflow, []int{-201, 202, 203, 204})
	elapsed := time.Since(start)

	if !strings.Contains(err.Error(), `stage "fan"`) || !strings.Contains(err.Error(), "synthetic child failure -201") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	if strings.Contains(err.Error(), "was cancelled") {
		t.Fatalf("workflow error = %v — cancelled siblings must not mask the original failure", err)
	}
	if elapsed > 3500*time.Millisecond {
		t.Errorf("stage failed after %v; cancellation must not wait out the queue's rate limiter", elapsed)
	}
	if got := cancelChildStarted.Load() - startedBefore; got != 1 {
		t.Errorf("children started = %d, want 1 (enqueued siblings must be cancelled without running)", got)
	}
	for _, n := range []int{202, 203, 204} {
		if s := childState(t, cancelChildID(n)); s != dbos.WorkflowStatusCancelled {
			t.Errorf("child %d state = %s, want CANCELLED", n, s)
		}
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName,
		"explode",
		"cancelMixChild", "cancelMixChild", "cancelMixChild", "cancelMixChild",
		"duro.cancel-watcher",
		"fan",
	})
}

// TestFanOutCancelPromptOnLateFailure proves detection is not gated on await
// order: the failing child is LAST in input order while its siblings block,
// yet the stage fails and cancels them long before they would finish.
func TestFanOutCancelPromptOnLateFailure(t *testing.T) {
	completedBefore := cancelChildCompleted.Load()

	start := time.Now()
	err, _ := mustFail(t, fanCancelWorkflow, []int{301, 302, 303, -304})
	elapsed := time.Since(start)

	if !strings.Contains(err.Error(), "synthetic child failure -304") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	if strings.Contains(err.Error(), "was cancelled") {
		t.Fatalf("workflow error = %v — cancelled siblings must not mask the original failure", err)
	}
	if limit := 4 * time.Second; elapsed > limit {
		t.Errorf("stage failed after %v, want < %v (failure of the last child must not wait for the children ahead of it)", elapsed, limit)
	}
	if got := cancelChildCompleted.Load() - completedBefore; got != 0 {
		t.Errorf("%d blocking children ran to completion, want 0 (they must be cancelled)", got)
	}
	for _, n := range []int{301, 302, 303} {
		if s := childState(t, cancelChildID(n)); s != dbos.WorkflowStatusCancelled {
			t.Errorf("child %d state = %s, want CANCELLED", n, s)
		}
	}
}

// TestFanOutDefaultDrainRunsSiblingsToCompletion proves the default is
// bit-for-bit unchanged: without the option, siblings of a failed child run
// to completion in the background.
func TestFanOutDefaultDrainRunsSiblingsToCompletion(t *testing.T) {
	doneBefore := cancelShortDone.Load()

	err, _ := mustFail(t, fanDrainWorkflow, []int{-401, 402, 403})
	if !strings.Contains(err.Error(), "synthetic child failure -401") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	eventually(t, 5*time.Second, "sibling children to run to completion", func() bool {
		return cancelShortDone.Load()-doneBefore == 2
	})
	for _, n := range []int{402, 403} {
		if s := childState(t, cancelChildID(n)); s != dbos.WorkflowStatusSuccess {
			t.Errorf("child %d state = %s, want SUCCESS (default must drain, not cancel)", n, s)
		}
	}
}

// TestFanOutCancelReplayAfterFailure is the recovery-replay proof: forking
// the failed parent from the await step re-executes only that step, which
// re-reads the children's terminal states — CANCELLED siblings sit earlier in
// input order than the ERROR child — and must re-surface the original error
// without re-running any child.
func TestFanOutCancelReplayAfterFailure(t *testing.T) {
	err, wfID := mustFail(t, fanCancelWorkflow, []int{501, -502, 503})
	if !strings.Contains(err.Error(), "synthetic child failure -502") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	startedBefore := cancelChildStarted.Load()

	handle, forkErr := dbos.ForkWorkflow[[]int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: wfID,
		StartStep:          stepIndex(t, stepNames(t, wfID), "fan"),
	})
	if forkErr != nil {
		t.Fatalf("forking workflow: %v", forkErr)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "synthetic child failure -502") {
		t.Fatalf("forked error = %v, want the original child failure (a CANCELLED sibling earlier in input order must not mask it)", err)
	}
	if got := cancelChildStarted.Load() - startedBefore; got != 0 {
		t.Errorf("%d children re-executed on replay, want 0 (the recovered stage must re-attach)", got)
	}
}

// TestFanOutCancelRecoveryMidCancellation recreates a crash between failure
// and cancellation: the failure is recorded but a sibling is live again
// (resumed). The recovered stage must observe the failure again, re-issue
// cancellation idempotently, and fail with the original error.
func TestFanOutCancelRecoveryMidCancellation(t *testing.T) {
	err, wfID := mustFail(t, fanCancelWorkflow, []int{601, -602, 603})
	if !strings.Contains(err.Error(), "synthetic child failure -602") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}

	if _, err := dbos.ResumeWorkflow[int](dctx, cancelChildID(601)); err != nil {
		t.Fatalf("resuming child: %v", err)
	}
	if s := childState(t, cancelChildID(601)); s == dbos.WorkflowStatusCancelled {
		t.Fatalf("child 601 still CANCELLED after resume")
	}

	handle, forkErr := dbos.ForkWorkflow[[]int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: wfID,
		StartStep:          stepIndex(t, stepNames(t, wfID), "fan"),
	})
	if forkErr != nil {
		t.Fatalf("forking workflow: %v", forkErr)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "synthetic child failure -602") {
		t.Fatalf("forked error = %v, want the original child failure", err)
	}
	if s := childState(t, cancelChildID(601)); s != dbos.WorkflowStatusCancelled {
		t.Errorf("resumed child state = %s, want CANCELLED (recovery must re-issue cancellation for live siblings)", s)
	}
}

// TestFanOutCancelWatcherCancelsWithoutParent proves cancellation does not
// depend on the parent's executor: the cancellation watcher — an independent
// queued workflow — detects the recorded failure and cancels live siblings
// on its own. The parent is terminal (dead, as far as the batch is
// concerned) throughout: after its failed run, its cancelled children are
// resumed and only the watcher is resumed to deal with them, standing in for
// a watcher recovered on another executor.
func TestFanOutCancelWatcherCancelsWithoutParent(t *testing.T) {
	err, wfID := mustFail(t, fanCancelWorkflow, []int{621, -622, 623})
	if !strings.Contains(err.Error(), "synthetic child failure -622") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	watcherID := cancelWatcherID(t, wfID)
	eventually(t, 5*time.Second, "the watcher to finish its own run", func() bool {
		return childState(t, watcherID) == dbos.WorkflowStatusSuccess
	})

	for _, n := range []int{621, 623} {
		if _, err := dbos.ResumeWorkflow[int](dctx, cancelChildID(n)); err != nil {
			t.Fatalf("resuming child %d: %v", n, err)
		}
	}
	// A completed workflow cannot be resumed; forking the watcher from step 0
	// starts a fresh run with the recorded input — the same full re-execution
	// a watcher recovered on another executor performs.
	rerun, err := dbos.ForkWorkflow[string](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: watcherID,
		StartStep:          0,
	})
	if err != nil {
		t.Fatalf("forking watcher: %v", err)
	}

	eventually(t, 10*time.Second, "the watcher to cancel the live siblings on its own", func() bool {
		return childState(t, cancelChildID(621)) == dbos.WorkflowStatusCancelled &&
			childState(t, cancelChildID(623)) == dbos.WorkflowStatusCancelled
	})
	if _, err := rerun.GetResult(); err != nil {
		t.Fatalf("re-run watcher failed: %v", err)
	}
}

// TestFanOutCancelOriginalErrorThroughRescue proves a Rescue around a
// cancel-enabled fan-out sees the triggering child's error as its cause,
// never a sibling's CANCELLED result.
func TestFanOutCancelOriginalErrorThroughRescue(t *testing.T) {
	fanRescueCause.Store("")

	result, _ := mustRun(t, fanCancelRescueWorkflow, []int{701, -702})
	if result != -1 {
		t.Fatalf("result = %d, want the handler fallback -1", result)
	}
	cause, _ := fanRescueCause.Load().(string)
	if !strings.Contains(cause, "synthetic child failure -702") {
		t.Fatalf("rescue cause = %q, want the failing child's error", cause)
	}
	if strings.Contains(cause, "was cancelled") {
		t.Fatalf("rescue cause = %q — cancelled siblings must not mask the original failure", cause)
	}
}

// TestFanOutCancelDoesNotCascadeToGrandchildren pins the documented nested
// behavior: cancelling a child does not cancel workflows the child had itself
// started — the grandchild of the cancelling stage runs to completion.
func TestFanOutCancelDoesNotCascadeToGrandchildren(t *testing.T) {
	grandchildBefore := cancelGrandchildDone.Load()

	err, _ := mustFail(t, fanCancelNestedWorkflow, []int{802, -803})
	if !strings.Contains(err.Error(), "synthetic child failure -803") {
		t.Fatalf("workflow error = %v, want the failing child's error", err)
	}
	if s := childState(t, cancelChildID(802)); s != dbos.WorkflowStatusCancelled {
		t.Fatalf("nested parent state = %s, want CANCELLED", s)
	}
	eventually(t, 8*time.Second, "the cancelled child's own fan-out child to run to completion", func() bool {
		return cancelGrandchildDone.Load()-grandchildBefore == 1
	})
}

// TestFanOutCancelChangesShapeFingerprint proves toggling WithCancelSiblings
// while a run is in flight trips the shape guard — the two modes checkpoint
// differently, so recovering across the toggle must fail fast instead of
// misreading checkpoints.
func TestFanOutCancelChangesShapeFingerprint(t *testing.T) {
	fanCancelToggle.Store(true)
	result, wfID := mustRun(t, fanShapeToggleWorkflow, []int{951})
	if len(result) != 1 || result[0] != 951*951 {
		t.Fatalf("result = %v, want [%d]", result, 951*951)
	}

	fanCancelToggle.Store(false)
	handle, err := dbos.ForkWorkflow[[]int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: wfID,
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

// --- Parallel --------------------------------------------------------------

// TestParallelCancelSiblingSteps proves in-flight sibling steps get their
// contexts cancelled promptly and the stage fails with the genuine error.
func TestParallelCancelSiblingSteps(t *testing.T) {
	parCancelObserved.Store(0)

	start := time.Now()
	err, _ := mustFail(t, parCancelWorkflow, []int{901, 902, -903})
	elapsed := time.Since(start)

	if !strings.Contains(err.Error(), `stage "par"`) || !strings.Contains(err.Error(), "synthetic step failure -903") {
		t.Fatalf("workflow error = %v, want the failing step's error", err)
	}
	if strings.Contains(err.Error(), "cancelled by a sibling step failure") {
		t.Fatalf("workflow error = %v — cancellation markers must not mask the original failure", err)
	}
	if limit := 3 * time.Second; elapsed > limit {
		t.Errorf("stage failed after %v, want < %v (siblings must be cancelled, not drained)", elapsed, limit)
	}
	if got := parCancelObserved.Load(); got != 2 {
		t.Errorf("steps that observed context cancellation = %d, want 2", got)
	}
}

// TestParallelCancelSkipsUnstartedSteps proves items whose steps have not
// started skip the function entirely — while still occupying their
// deterministic step slots in the checkpoint layout.
func TestParallelCancelSkipsUnstartedSteps(t *testing.T) {
	parSkipRuns.Store(0)

	err, wfID := mustFail(t, parSkipWorkflow, []int{-911, 912, 913})
	if !strings.Contains(err.Error(), "synthetic step failure -911") {
		t.Fatalf("workflow error = %v, want the failing step's error", err)
	}
	if got := parSkipRuns.Load(); got != 1 {
		t.Errorf("step function executions = %d, want 1 (unstarted items must be skipped)", got)
	}
	assertNames(t, stepNames(t, wfID), []string{
		duro.ShapeStepName, "explode", "par", "par", "par",
	})
}

// TestParallelCancelRescueForkReplay is the step-alignment proof: a rescued,
// cancel-enabled Parallel replayed from its handler step must reproduce the
// identical outcome — skipped/cancelled slots replay as cancellations, the
// genuine error re-surfaces as the cause, and no step function re-runs. If
// cancelled items did not occupy deterministic step slots, the handler's step
// ID would misalign here.
func TestParallelCancelRescueForkReplay(t *testing.T) {
	parRescueRuns.Store(0)
	parRescueCause.Store("")

	result, wfID := mustRun(t, parCancelRescueWorkflow, []int{921, -922, 923})
	if result != -1 {
		t.Fatalf("result = %d, want the handler fallback -1", result)
	}
	liveCause, _ := parRescueCause.Load().(string)
	if !strings.Contains(liveCause, "synthetic step failure -922") {
		t.Fatalf("rescue cause = %q, want the failing step's error", liveCause)
	}
	if strings.Contains(liveCause, "cancelled by a sibling step failure") {
		t.Fatalf("rescue cause = %q — cancellation markers must not mask the original failure", liveCause)
	}
	runsAfterLive := parRescueRuns.Load()

	parRescueCause.Store("")
	handle, err := dbos.ForkWorkflow[int](dctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: wfID,
		StartStep:          stepIndex(t, stepNames(t, wfID), "save"),
	})
	if err != nil {
		t.Fatalf("forking workflow: %v", err)
	}
	forked, err := handle.GetResult()
	if err != nil {
		t.Fatalf("forked workflow failed: %v", err)
	}
	if forked != -1 {
		t.Errorf("forked result = %d, want -1", forked)
	}
	if got := parRescueRuns.Load(); got != runsAfterLive {
		t.Errorf("step executions changed on replay: %d → %d (steps re-executed instead of replayed)", runsAfterLive, got)
	}
	replayCause, _ := parRescueCause.Load().(string)
	if replayCause != liveCause {
		t.Errorf("replayed cause %q differs from live cause %q — the rescue decision must be replay-stable", replayCause, liveCause)
	}
}

// --- construction guards ---------------------------------------------------

// TestCancelSiblingStepsRejectedOutsideParallel proves the misuse guard:
// WithCancelSiblingSteps panics at construction time on every other stage.
func TestCancelSiblingStepsRejectedOutsideParallel(t *testing.T) {
	opt := duro.WithCancelSiblingSteps()
	echo := func(_ context.Context, n int) (int, error) { return n, nil }
	pass := duro.Pipe1(duro.Step("id", echo))

	assertPanics(t, "Step", func() { duro.Step("s", echo, opt) })
	assertPanics(t, "Tap", func() { duro.Tap("t", func(_ context.Context, _ int) error { return nil }, opt) })
	assertPanics(t, "Filter", func() { duro.Filter("f", func(_ context.Context, _ int) (bool, error) { return true, nil }, opt) })
	assertPanics(t, "Expand", func() { duro.Expand("e", func(_ context.Context, n int) ([]int, error) { return []int{n}, nil }, opt) })
	assertPanics(t, "Reduce", func() { duro.Reduce("r", func(_ context.Context, acc, _ int) (int, error) { return acc, nil }, 0, opt) })
	assertPanics(t, "Collect", func() { duro.Collect[int]("c", opt) })
	assertPanics(t, "Branch", func() {
		duro.Branch("b", func(_ context.Context, _ int) (bool, error) { return true, nil }, pass, pass, opt)
	})
	assertPanics(t, "Loop", func() {
		duro.Loop("l", pass, func(_ context.Context, _ int) (bool, error) { return true, nil }, opt)
	})
	assertPanics(t, "Rescue", func() {
		duro.Rescue("x", pass, func(_ context.Context, n int, _ error) (int, error) { return n, nil }, opt)
	})

	// Parallel accepts it.
	duro.Parallel("ok", 1, echo, opt)
}

// TestCancelWatchIntervalValidation proves WithCancelWatchInterval fails fast
// at construction: it is meaningless without WithCancelSiblings, and a
// non-positive duration is never a valid cadence.
func TestCancelWatchIntervalValidation(t *testing.T) {
	child := duro.Workflow(cancelShortChild)
	assertPanics(t, "FanOut/interval-without-cancel", func() {
		duro.FanOut("f", cancelWideQueue, child, duro.WithCancelWatchInterval(time.Second))
	})
	assertPanics(t, "FanOut/non-positive-interval", func() {
		duro.FanOut("f", cancelWideQueue, child, duro.WithCancelSiblings(), duro.WithCancelWatchInterval(0))
	})
	assertPanics(t, "FanOut/negative-interval", func() {
		duro.FanOut("f", cancelWideQueue, child, duro.WithCancelSiblings(), duro.WithCancelWatchInterval(-time.Second))
	})

	// The valid combination constructs.
	duro.FanOut("f", cancelWideQueue, child, duro.WithCancelSiblings(), duro.WithCancelWatchInterval(time.Second))
}
