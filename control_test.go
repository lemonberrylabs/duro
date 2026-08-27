package duro_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- control-flow (Cancel/Resume) test fixtures ------------------------------

// gateWorkflow parks in its first stage until the test opens the gate, then
// runs a second stage. It gives the control tests a run that is verifiably
// live — pending, mid-stage — and that leaves cleanly once released: DBOS
// checks for cancellation at the start of every step, so a cancelled run
// records "gate" when it returns and stops at "after". Unlike a run parked in
// Recv, no goroutine lingers to race a later Resume.
var (
	gateMu    sync.Mutex
	gateCh    chan struct{}
	gateRuns  atomic.Int64
	afterRuns atomic.Int64
)

// armGate installs a fresh, closed-on-release gate and resets the counters.
func armGate() {
	gateMu.Lock()
	defer gateMu.Unlock()
	gateCh = make(chan struct{})
	gateRuns.Store(0)
	afterRuns.Store(0)
}

func openGate() {
	gateMu.Lock()
	defer gateMu.Unlock()
	close(gateCh)
}

func currentGate() chan struct{} {
	gateMu.Lock()
	defer gateMu.Unlock()
	return gateCh
}

func gateWorkflow(ctx dbos.Context, n int) (int, error) {
	return duro.Run(ctx, n, duro.Pipe2(
		duro.Step("gate", func(ctx context.Context, v int) (int, error) {
			gateRuns.Add(1)
			select {
			case <-currentGate():
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return v + 1, nil
		}),
		duro.Step("after", func(_ context.Context, v int) (int, error) {
			afterRuns.Add(1)
			return v * 10, nil
		}),
	))
}

func registerControlWorkflows(ctx dbos.Context) {
	dbos.RegisterWorkflow(ctx, gateWorkflow, dbos.WithWorkflowName("gateWorkflow"))
}

// awaitState polls Status until pred holds for the run's state, failing the
// test after 10 seconds.
func awaitState(t *testing.T, id string, pred func(duro.State) bool) duro.RunStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, err := duro.Status(app, id)
		if err != nil {
			t.Fatalf("Status(%s): %v", id, err)
		}
		if pred(s.State) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached the awaited state (last: %s)", id, s.State)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startPinnedToNobody enqueues a pipeline run under an application version no
// executor has, so it exists but never starts — the enqueued-and-waiting case.
func startPinnedToNobody(t *testing.T, id string) {
	t.Helper()
	queueOpt, err := fanQueue.WorkflowOption(app)
	if err != nil {
		t.Fatalf("resolving queue: %v", err)
	}
	if _, err := registeredWf.Start(app, 1,
		dbos.WithWorkflowID(id),
		queueOpt,
		dbos.WithApplicationVersion("control-version-nobody-has")); err != nil {
		t.Fatalf("enqueuing pinned run: %v", err)
	}
}

// --- tests ------------------------------------------------------------------

// TestCancelPendingRun proves Cancel stops a live run at its next stage
// boundary: the stage in flight completes and is checkpointed, the next one
// never runs, and the run lands in StateCancelled with its queue cleared.
func TestCancelPendingRun(t *testing.T) {
	armGate()
	const id = "control-cancel-pending"
	h, err := dbos.RunWorkflow(dctx, gateWorkflow, 1, dbos.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	for gateRuns.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if s, err := duro.Status(app, id); err != nil || s.State != duro.StatePending {
		t.Fatalf("status before cancel = %+v, %v; want pending", s, err)
	}

	if err := duro.Cancel(app, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	openGate()
	if _, err := h.GetResult(); err == nil {
		t.Fatal("expected the cancelled run to finish with an error")
	}

	s := awaitState(t, id, duro.State.Terminal)
	if s.State != duro.StateCancelled || s.Err == nil {
		t.Errorf("state = %s (err %v), want cancelled with a synthesized error", s.State, s.Err)
	}
	if afterRuns.Load() != 0 {
		t.Errorf("'after' ran %d times after cancellation, want 0", afterRuns.Load())
	}
	steps, err := duro.Steps(app, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	if len(steps) != 2 || steps[1].Name != "gate" {
		t.Errorf("steps = %+v, want [%s gate]: the in-flight stage checkpoints, the next never starts", steps, duro.ShapeStepName)
	}

	if err := duro.Cancel(app, id); !errors.Is(err, duro.ErrRunTerminal) {
		t.Errorf("second Cancel error = %v, want ErrRunTerminal", err)
	}
}

// TestCancelEnqueuedRun proves an enqueued run that never started cancels
// immediately, and that Cancel refuses IDs it cannot act on.
func TestCancelEnqueuedRun(t *testing.T) {
	const id = "control-cancel-enqueued"
	startPinnedToNobody(t, id)
	if s, _ := duro.Status(app, id); s.State != duro.StateEnqueued || s.QueueName != fanQueueName {
		t.Fatalf("status = %+v, want enqueued on %s", s, fanQueueName)
	}

	if err := duro.Cancel(app, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	s, err := duro.Status(app, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != duro.StateCancelled || s.QueueName != "" {
		t.Errorf("status = %+v, want cancelled with the queue cleared", s)
	}

	done, err := registeredWf.Start(app, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := done.Result(); err != nil {
		t.Fatal(err)
	}
	if err := duro.Cancel(app, done.ID()); !errors.Is(err, duro.ErrRunTerminal) || !strings.Contains(err.Error(), "success") {
		t.Errorf("Cancel(success run) = %v, want ErrRunTerminal naming the state", err)
	}
	if err := duro.Cancel(app, "control-no-such-run"); !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Cancel(unknown) = %v, want ErrRunNotFound", err)
	}
	if err := duro.Cancel(app, ""); err == nil || !strings.Contains(err.Error(), "Cancel requires") {
		t.Errorf("Cancel(\"\") = %v, want a usage error", err)
	}
}

// TestResumeRefusesRunCancelledMidExecution proves the quiescence rule: a run
// cancelled while a stage is executing cannot be resumed under its ID — not
// while the stage is still running, and not after, because DBOS records
// nothing that says when the old executor stopped. Resume refuses with
// ErrRunInFlight both times, the cancelled run stays cancelled and never runs
// its next stage, and ForkFromStage is the way to carry the work on: a new
// run that replays the checkpoints and finishes.
func TestResumeRefusesRunCancelledMidExecution(t *testing.T) {
	armGate()
	const id = "control-resume-mid-execution"
	h, err := dbos.RunWorkflow(dctx, gateWorkflow, 4, dbos.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	for gateRuns.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if err := app.Resume(context.Background(), id); !errors.Is(err, duro.ErrRunActive) {
		t.Errorf("Resume(pending run) = %v, want ErrRunActive", err)
	}

	if err := duro.Cancel(app, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	// The gate is still held: the in-flight stage is running right now.
	err = app.Resume(context.Background(), id)
	if !errors.Is(err, duro.ErrRunInFlight) || !strings.Contains(err.Error(), "ForkFromStage") {
		t.Errorf("Resume(cancelled, stage in flight) = %v, want ErrRunInFlight pointing at ForkFromStage", err)
	}
	if s, _ := duro.Status(app, id); s.State != duro.StateCancelled {
		t.Errorf("state after refused Resume = %s, want still cancelled", s.State)
	}

	openGate()
	_, _ = h.GetResult() // the original goroutine exits at "after"
	awaitState(t, id, duro.State.Terminal)
	if afterRuns.Load() != 0 {
		t.Errorf("'after' ran %d times on the cancelled run, want 0", afterRuns.Load())
	}
	// Still refused: the old executor is gone, but nothing in the database
	// says so, and Resume must not guess.
	if err := app.Resume(context.Background(), id); !errors.Is(err, duro.ErrRunInFlight) {
		t.Errorf("Resume(cancelled, stage finished) = %v, want ErrRunInFlight", err)
	}

	forked, err := duro.ForkFromStage[int](app, duro.Fork{WorkflowID: id, Stage: "after"})
	if err == nil {
		t.Fatalf("ForkFromStage from a stage the run never recorded succeeded (%s); want an error", forked.ID())
	}
	forked, err = duro.ForkFromStage[int](app, duro.Fork{WorkflowID: id, Stage: "gate"})
	if err != nil {
		t.Fatalf("ForkFromStage: %v", err)
	}
	result, err := forked.Result()
	if err != nil {
		t.Fatalf("forked run failed: %v", err)
	}
	if result != 50 || forked.ID() == id {
		t.Errorf("fork result/ID = %d/%s, want 50 under a new ID", result, forked.ID())
	}
	if afterRuns.Load() != 1 {
		t.Errorf("'after' ran %d times across both runs, want 1 (the fork)", afterRuns.Load())
	}
	if s, _ := duro.Status(app, id); s.State != duro.StateCancelled {
		t.Errorf("original run after fork = %s, want still cancelled", s.State)
	}
	if s, _ := duro.Status(app, forked.ID()); s.ForkedFrom != id || s.State != duro.StateSuccess {
		t.Errorf("forked run status = %+v, want success forked from %s", s, id)
	}
}

// TestResumeCancelledBeforeStart proves the allowed cancelled case: a run
// cancelled while still waiting on its queue never executed, so it resumes
// under its ID — and, once an executor on its version exists, completes.
func TestResumeCancelledBeforeStart(t *testing.T) {
	const id = "control-resume-never-started"
	startPinnedToNobody(t, id)
	if err := duro.Cancel(app, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if s, _ := duro.Status(app, id); s.State != duro.StateCancelled || s.Attempts != 0 {
		t.Fatalf("status after cancel = %+v, want cancelled with 0 attempts", s)
	}
	if err := app.Resume(context.Background(), id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	s, err := duro.Status(app, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != duro.StateEnqueued || s.Attempts != 0 {
		t.Errorf("status after Resume = %+v, want enqueued again with 0 attempts", s)
	}
	if err := app.Resume(context.Background(), id); !errors.Is(err, duro.ErrRunActive) {
		t.Errorf("second Resume = %v, want ErrRunActive", err)
	}
	if err := duro.Cancel(app, id); err != nil {
		t.Fatal(err)
	}

	if err := app.Resume(context.Background(), "control-no-such-run"); !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Resume(unknown) = %v, want ErrRunNotFound", err)
	}
}

// TestAppResumeIsBounded proves the engine-side Resume honours the caller's
// context: a cancelled context fails it fast, and with the workflow table
// locked a request deadline ends it as a deadline.
func TestAppResumeIsBounded(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := app.Resume(cancelled, "control-anything"); !errors.Is(err, context.Canceled) {
		t.Errorf("Resume on a cancelled context = %v, want one wrapping context.Canceled", err)
	}

	conn := wpConn(t)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE dbos.workflow_status IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("locking: %v", err)
	}
	ctx, cancelDeadline := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelDeadline()
	start := time.Now()
	err = app.Resume(ctx, "control-anything")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Resume against a locked table = %v, want one wrapping context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("returned after %v, want about the 200ms deadline", elapsed)
	}
}

// TestResumeErrorRunIsTerminal proves a run that failed with an error is not
// resumable — that is ForkFromStage's job — and says so instead of no-op'ing.
func TestResumeErrorRunIsTerminal(t *testing.T) {
	h, err := dbos.RunWorkflow(dctx, retryPredicateWorkflow, "permanent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.GetResult(); err == nil {
		t.Fatal("expected the run to fail")
	}
	err = app.Resume(context.Background(), h.GetWorkflowID())
	if !errors.Is(err, duro.ErrRunTerminal) || !strings.Contains(err.Error(), "ForkFromStage") {
		t.Errorf("Resume(error run) = %v, want ErrRunTerminal pointing at ForkFromStage", err)
	}
}

// TestResumeEnqueuedRunIsActive proves a run that is merely waiting on its
// queue counts as active: Resume must not yank it past the queue.
func TestResumeEnqueuedRunIsActive(t *testing.T) {
	const id = "control-resume-enqueued"
	startPinnedToNobody(t, id)
	if err := app.Resume(context.Background(), id); !errors.Is(err, duro.ErrRunActive) {
		t.Errorf("Resume(enqueued run) = %v, want ErrRunActive", err)
	}
	if err := duro.Cancel(app, id); err != nil {
		t.Fatal(err)
	}
}

// flipToRetriesExceeded moves a finished run into DBOS's dead-letter state
// directly in the database — the state DBOS assigns after a run crashes its
// executor more than the allowed number of times, which no test can reach
// quickly — keeping every other column as DBOS left it.
func flipToRetriesExceeded(t *testing.T, id string) {
	t.Helper()
	conn := wpConn(t)
	if _, err := conn.Exec(context.Background(),
		"UPDATE dbos.workflow_status SET status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED' WHERE workflow_uuid = $1", id); err != nil {
		t.Fatalf("flipping %s to retries-exceeded: %v", id, err)
	}
}

func queueNameOf(t *testing.T, id string) string {
	t.Helper()
	conn := wpConn(t)
	var queue *string
	if err := conn.QueryRow(context.Background(),
		"SELECT queue_name FROM dbos.workflow_status WHERE workflow_uuid = $1", id).Scan(&queue); err != nil {
		t.Fatalf("reading queue of %s: %v", id, err)
	}
	if queue == nil {
		return "<NULL>"
	}
	return *queue
}

// TestResumeReturnsRunToItsQueue pins the queue rule: a retries-exceeded run
// that still records its queue is resumed onto that queue — never DBOS's
// internal one, which would exempt it from the queue's limits — and its
// checkpointed stages replay rather than re-execute. It also proves the
// retries_exceeded state round-trips through Status and a WithStates filter.
func TestResumeReturnsRunToItsQueue(t *testing.T) {
	queueOpt, err := fanQueue.WorkflowOption(app)
	if err != nil {
		t.Fatalf("resolving queue: %v", err)
	}
	h, err := registeredWf.Start(app, 5, queueOpt)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	if _, err := h.Result(); err != nil {
		t.Fatal(err)
	}
	id := h.ID()
	flipToRetriesExceeded(t, id)

	s, err := duro.Status(app, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != duro.StateRetriesExceeded || s.Err == nil || s.QueueName != fanQueueName {
		t.Fatalf("status = %+v, want retries_exceeded (with an error) on %s", s, fanQueueName)
	}
	listed, err := duro.ListRuns(app, duro.WithIDs(id), duro.WithStates(duro.StateRetriesExceeded))
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Err == nil || listed[0].Err.Error() != "duro: run retries_exceeded" {
		t.Errorf("ListRuns(WithStates(retries_exceeded)) = %+v, want the run with its synthesized error", listed)
	}
	before, err := duro.Steps(app, id)
	if err != nil {
		t.Fatal(err)
	}

	if err := app.Resume(context.Background(), id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if q := queueNameOf(t, id); q != fanQueueName {
		t.Errorf("queue after resume = %q, want %q (the run's own queue)", q, fanQueueName)
	}
	s = awaitState(t, id, duro.State.Terminal)
	if s.State != duro.StateSuccess || s.Attempts != 1 {
		t.Errorf("status after resume = %+v, want success with the attempt count restarted at 1", s)
	}
	after, err := duro.Steps(app, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("steps after resume = %+v, want the same %d steps as before", after, len(before))
	}
	for i := range before {
		if after[i].Name != before[i].Name || !after[i].StartedAt.Equal(before[i].StartedAt) {
			t.Errorf("step %d after resume = %+v, want the original checkpoint %+v (replayed, not re-run)", i, after[i], before[i])
		}
	}
}

// TestResumeDirectRunUsesInternalQueue proves the fallback: a run started
// directly (no queue recorded) resumes on DBOS's internal queue and completes.
func TestResumeDirectRunUsesInternalQueue(t *testing.T) {
	h, err := registeredWf.Start(app, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Result(); err != nil {
		t.Fatal(err)
	}
	id := h.ID()
	flipToRetriesExceeded(t, id)

	if err := app.Resume(context.Background(), id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if q := queueNameOf(t, id); q != "_dbos_internal_queue" {
		t.Errorf("queue after resume = %q, want DBOS's internal queue", q)
	}
	if s := awaitState(t, id, duro.State.Terminal); s.State != duro.StateSuccess {
		t.Errorf("state after resume = %s, want success", s.State)
	}
}
