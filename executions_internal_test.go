package duro

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTrackedApp launches a worker-pool App with fast cadences under a
// per-test application name. register runs between New and Launch.
func newTrackedApp(t *testing.T, executorID string, register func(a *App)) *App {
	t.Helper()
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	a, err := New(context.Background(), Config{
		Name:               "duro-exec-" + t.Name(),
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ExecutorID:         executorID,
		ApplicationVersion: "exec-v1",
	}, WithWorkerPool(
		WithHeartbeatInterval(100*time.Millisecond),
		WithStaleThreshold(1*time.Second),
	), WithSweepInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("New %s: %v", executorID, err)
	}
	register(a)
	if err := a.Launch(); err != nil {
		t.Fatalf("Launch %s: %v", executorID, err)
	}
	return a
}

func unsettledIDs(t *testing.T, a *App, ids ...string) map[string]UnsettledRun {
	t.Helper()
	runs, err := a.Unsettled(context.Background(), ids...)
	if err != nil {
		t.Fatalf("Unsettled: %v", err)
	}
	out := make(map[string]UnsettledRun, len(runs))
	for _, r := range runs {
		out[r.ID] = r
	}
	return out
}

// ctxWaitStep parks until its context ends and reports the cause: a stage
// that honors cancellation.
type ctxWaitStep struct {
	started atomic.Int64
	cause   atomic.Value // error
}

func (s *ctxWaitStep) run(ctx context.Context, in int) (int, error) {
	s.started.Add(1)
	<-ctx.Done()
	s.cause.Store(context.Cause(ctx))
	return 0, ctx.Err()
}

// TestCancelInterruptsTrackedStage is the cancellation half of execution
// tracking: cancelling a run interrupts the stage it is executing within a
// heartbeat or so, stops that stage's retries, and runs nothing after it —
// and Unsettled goes from "executing" to settled.
func TestCancelInterruptsTrackedStage(t *testing.T) {
	var wait ctxWaitStep
	var post atomic.Int64
	var wf *PipelineWorkflow[int, int]
	a := newTrackedApp(t, "exec-cancel-A", func(a *App) {
		wf = Register(a, "exec-cancel", Pipe3(
			Step("pre", func(_ context.Context, in int) (int, error) { return in, nil }),
			// Retries that would keep the stage going for minutes if
			// cancellation did not reach the retry loop.
			Step("wait", wait.run, WithMaxRetries(5), WithBaseInterval(time.Minute)),
			Step("post", func(_ context.Context, in int) (int, error) { post.Add(1); return in, nil }),
		))
	})
	defer a.Close(5 * time.Second)

	h, err := wf.Start(a, 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	id := h.ID()
	waitUntil(t, 5*time.Second, "the wait stage to start", func() bool { return wait.started.Load() == 1 })

	got := unsettledIDs(t, a, id)
	if r, ok := got[id]; !ok || r.State != StatePending || !r.Executing {
		t.Fatalf("while running: Unsettled = %+v, want PENDING and executing", got)
	}

	if err := Cancel(a, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitUntil(t, 3*time.Second, "the cancelled run to settle", func() bool {
		return len(unsettledIDs(t, a, id)) == 0
	})
	if cause, _ := wait.cause.Load().(error); !errors.Is(cause, errRunCancelled) {
		t.Errorf("stage context cause = %v, want errRunCancelled", cause)
	}
	if n := wait.started.Load(); n != 1 {
		t.Errorf("wait stage attempts = %d, want 1 (cancellation must stop the retry loop)", n)
	}
	if n := post.Load(); n != 0 {
		t.Errorf("post stage ran %d times after cancellation, want 0", n)
	}
	if s, err := Status(a, id); err != nil || s.State != StateCancelled {
		t.Errorf("status = %+v, %v; want CANCELLED", s, err)
	}
}

// TestUnsettledWhileUncooperativeStageRuns: a stage that ignores its context
// keeps its cancelled run unsettled for as long as it runs, however long that
// is, and the run settles as soon as it returns.
func TestUnsettledWhileUncooperativeStageRuns(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)
	var wf *PipelineWorkflow[int, int]
	a := newTrackedApp(t, "exec-block-A", func(a *App) { wf = Register(a, "exec-block", wpBlockingPipeline()) })
	defer a.Close(5 * time.Second)

	h, err := wf.Start(a, 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	id := h.ID()
	waitUntil(t, 5*time.Second, "the block stage to start", func() bool { return wpBlockCount.Load() == 1 })
	if err := Cancel(a, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// Several polls and a full stale threshold: time does not settle it.
	time.Sleep(1500 * time.Millisecond)
	got := unsettledIDs(t, a, id)
	if r, ok := got[id]; !ok || r.State != StateCancelled || !r.Executing {
		t.Fatalf("stage still running: Unsettled = %+v, want CANCELLED and executing", got)
	}

	wpReleaseBlocker()
	waitUntil(t, 3*time.Second, "the run to settle once its stage returns", func() bool {
		return len(unsettledIDs(t, a, id)) == 0
	})
	if n := wpPostCount.Load(); n != 0 {
		t.Errorf("post stage ran %d times after cancellation, want 0", n)
	}
}

// TestUnsettledCrashedExecutorSettles: an execution left open by a process
// that died stops counting once the executor's lease goes stale.
func TestUnsettledCrashedExecutorSettles(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)
	var wfA *PipelineWorkflow[int, int]
	appA := newTrackedApp(t, "exec-crash-A", func(a *App) { wfA = Register(a, "exec-crash", wpBlockingPipeline()) })
	appB := newTrackedApp(t, "exec-crash-B", func(a *App) { Register(a, "exec-crash", wpBlockingPipeline()) })
	defer appB.Close(5 * time.Second)

	h, err := wfA.Start(appA, 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	id := h.ID()
	waitUntil(t, 5*time.Second, "the block stage to start", func() bool { return wpBlockCount.Load() == 1 })
	if err := Cancel(appA, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	crashApp(appA) // no tombstone, and the stage never closes its row

	if r, ok := unsettledIDs(t, appB, id)[id]; !ok || !r.Executing {
		t.Fatalf("right after the crash: want the run executing (lease still fresh), got %+v", r)
	}
	waitUntil(t, 5*time.Second, "the dead executor's lease to go stale", func() bool {
		return len(unsettledIDs(t, appB, id)) == 0
	})
}

// TestUnsettledUntrackedWorkflow: a hand-written workflow records no
// execution, so once cancelled after starting it counts as executing for as
// long as its executor lives, and settles when the executor stops.
func TestUnsettledUntrackedWorkflow(t *testing.T) {
	release := make(chan struct{})
	var started atomic.Int64
	handWritten := func(ctx dbos.Context, in int) (int, error) {
		return dbos.RunAsStep(ctx, func(context.Context) (int, error) {
			started.Add(1)
			<-release
			return in, nil
		}, dbos.WithStepName("hand-written-block"))
	}
	a := newTrackedApp(t, "exec-untracked-A", func(a *App) {
		dbos.RegisterWorkflow(a.Context(), handWritten, dbos.WithWorkflowName("exec-untracked"))
	})
	closed := false
	defer func() {
		if !closed {
			a.Close(5 * time.Second)
		}
	}()

	h, err := dbos.RunWorkflow(a.Context(), handWritten, 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	id := h.GetWorkflowID()
	waitUntil(t, 5*time.Second, "the hand-written step to start", func() bool { return started.Load() == 1 })

	if r, ok := unsettledIDs(t, a, id)[id]; !ok || r.State != StatePending || r.Executing {
		t.Fatalf("running untracked: want PENDING and not executing (no row, not cancelled), got %+v", r)
	}
	if err := Cancel(a, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(release)
	time.Sleep(300 * time.Millisecond) // the step has returned; the run is still untracked
	if r, ok := unsettledIDs(t, a, id)[id]; !ok || r.State != StateCancelled || !r.Executing {
		t.Fatalf("cancelled untracked run on a live executor: want CANCELLED and executing, got %+v", r)
	}

	a.Close(5 * time.Second) // tombstones the lease
	closed = true
	checker := newTrackedApp(t, "exec-untracked-B", func(*App) {})
	defer checker.Close(5 * time.Second)
	if got := unsettledIDs(t, checker, id); len(got) != 0 {
		t.Errorf("after its executor stopped: Unsettled = %+v, want settled", got)
	}
}

// TestUnsettledFanOutCancelSiblings combines tracking with FanOut's sibling
// cancellation: one child fails, duro cancels the other, the tracker
// interrupts the cancelled child's running stage, and the whole tree settles.
func TestUnsettledFanOutCancelSiblings(t *testing.T) {
	var wait ctxWaitStep
	queue := NewQueue("exec-fan-queue")
	var parent *PipelineWorkflow[int, []int]
	a := newTrackedApp(t, "exec-fan-A", func(a *App) {
		child := Register(a, "exec-fan-child", Pipe1(
			Step("child", func(ctx context.Context, in int) (int, error) {
				if in == 1 {
					return 0, errors.New("child 1 fails")
				}
				return wait.run(ctx, in)
			}),
		))
		parent = Register(a, "exec-fan-parent", Pipe3(
			Expand("items", func(context.Context, int) ([]int, error) { return []int{1, 2}, nil }),
			FanOut("fan", queue, child, WithCancelSiblings()),
			Collect[int]("collect"),
		))
	})
	defer a.Close(5 * time.Second)

	h, err := parent.Start(a, 0)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.Result(); err == nil {
		t.Fatal("parent succeeded, want the child failure")
	}

	ids := []string{h.ID()}
	steps, err := Steps(a, h.ID())
	if err != nil {
		t.Fatalf("steps: %v", err)
	}
	for _, s := range steps {
		if s.ChildID != "" {
			ids = append(ids, s.ChildID)
		}
	}
	waitUntil(t, 5*time.Second, "the whole tree to settle", func() bool {
		return len(unsettledIDs(t, a, ids...)) == 0
	})
	if cause, _ := wait.cause.Load().(error); !errors.Is(cause, errRunCancelled) {
		t.Errorf("cancelled child's stage cause = %v, want errRunCancelled", cause)
	}
}

// TestParallelCancelInterruptsSteps: cancellation reaches Parallel's
// concurrent steps too, since they run on the tracked run's context.
func TestParallelCancelInterruptsSteps(t *testing.T) {
	var wait ctxWaitStep
	var wf *PipelineWorkflow[int, []int]
	a := newTrackedApp(t, "exec-par-A", func(a *App) {
		wf = Register(a, "exec-par", Pipe3(
			Expand("items", func(context.Context, int) ([]int, error) { return []int{1, 2, 3}, nil }),
			Parallel("wait", Unbounded, wait.run),
			Collect[int]("collect"),
		))
	})
	defer a.Close(5 * time.Second)

	h, err := wf.Start(a, 0)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitUntil(t, 5*time.Second, "all parallel steps to start", func() bool { return wait.started.Load() == 3 })
	if err := Cancel(a, h.ID()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitUntil(t, 3*time.Second, "the run to settle", func() bool { return len(unsettledIDs(t, a, h.ID())) == 0 })
}

// TestParallelDrainsLaunchedStepsOnUpstreamError: when an upstream stage
// fails after Parallel launched steps, the run does not return until those
// steps have — no step outlives its workflow.
func TestParallelDrainsLaunchedStepsOnUpstreamError(t *testing.T) {
	var finished atomic.Bool
	var wf *PipelineWorkflow[int, []int]
	a := newTrackedApp(t, "exec-drain-A", func(a *App) {
		wf = Register(a, "exec-drain", Pipe4(
			Expand("items", func(context.Context, int) ([]int, error) { return []int{1, 2}, nil }),
			Step("gate", func(_ context.Context, in int) (int, error) {
				if in == 2 {
					return 0, errors.New("gate rejects 2")
				}
				return in, nil
			}),
			Parallel("slow", Unbounded, func(_ context.Context, in int) (int, error) {
				time.Sleep(400 * time.Millisecond)
				finished.Store(true)
				return in, nil
			}),
			Collect[int]("collect"),
		))
	})
	defer a.Close(5 * time.Second)

	h, err := wf.Start(a, 0)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.Result(); err == nil {
		t.Fatal("run succeeded, want the gate failure")
	}
	if !finished.Load() {
		t.Fatal("the run failed while a launched parallel step was still running")
	}
	if got := unsettledIDs(t, a, h.ID()); len(got) != 0 {
		t.Errorf("Unsettled = %+v, want settled", got)
	}
}

// TestUnsettledPredicate drives the settled-or-not decision against seeded
// rows: every combination of run state, execution row and executor lease.
func TestUnsettledPredicate(t *testing.T) {
	ctx := context.Background()
	pool := wpConnFor(t)
	wp := &workerPool{pool: pool, logger: wpLogger(), staleThreshold: time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	const p = "exec-pred-"
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'exec-pred-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'exec-pred-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_run_executions WHERE executor_id LIKE 'exec-pred-%'")
	})

	lease := func(id string, last time.Time, staleAfter time.Duration) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO dbos.duro_executor_heartbeats
			(executor_id, app_name, application_version, last_heartbeat, stale_after_ms)
			VALUES ($1, 'exec-pred', 'v1', $2, $3)
			ON CONFLICT (executor_id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat, stale_after_ms = EXCLUDED.stale_after_ms`,
			p+id, last, staleAfter.Milliseconds()); err != nil {
			t.Fatalf("lease %s: %v", id, err)
		}
	}
	run := func(id, status, executor string, attempts int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO dbos.workflow_status
			(workflow_uuid, status, executor_id, application_version, recovery_attempts)
			VALUES ($1, $2, $3, 'v1', $4)`, p+id, status, p+executor, attempts); err != nil {
			t.Fatalf("run %s: %v", id, err)
		}
	}
	execution := func(runID, executor string, open bool) {
		t.Helper()
		var ended any
		if !open {
			ended = time.Now()
		}
		if _, err := pool.Exec(ctx, `INSERT INTO dbos.duro_run_executions
			(executor_id, workflow_uuid, token, started_at, ended_at) VALUES ($1, $2, 't', now(), $3)`,
			p+executor, p+runID, ended); err != nil {
			t.Fatalf("execution %s: %v", runID, err)
		}
	}

	now := time.Now()
	lease("live", now, time.Minute)
	lease("stale", now.Add(-2*time.Hour), time.Minute)
	lease("tomb", time.Unix(0, 0), time.Minute)
	lease("slow-live", now.Add(-30*time.Second), time.Minute)    // within its own threshold
	lease("fast-dead", now.Add(-30*time.Second), 10*time.Second) // past its own threshold

	cases := []struct {
		id, status, executor string
		attempts             int
		row                  string // "", "open", "ended"; on the run's executor unless rowOn
		rowOn                string
		wantUnsettled        bool
		wantExecuting        bool
		why                  string
	}{
		{"pending", "PENDING", "live", 1, "", "", true, false, "not final"},
		{"enqueued", "ENQUEUED", "live", 0, "", "", true, false, "not final"},
		{"success-open-live", "SUCCESS", "live", 1, "open", "", true, true, "open execution on a live executor"},
		{"cancelled-open-live", "CANCELLED", "live", 1, "open", "", true, true, "stage still running"},
		{"cancelled-open-stale", "CANCELLED", "stale", 1, "open", "", false, false, "its executor is dead"},
		{"cancelled-open-tomb", "CANCELLED", "tomb", 1, "open", "", false, false, "its executor shut down"},
		{"cancelled-open-slow", "CANCELLED", "slow-live", 1, "open", "", true, true, "judged by the executor's own threshold: alive"},
		{"cancelled-open-fast", "CANCELLED", "fast-dead", 1, "open", "", false, false, "judged by the executor's own threshold: dead"},
		{"cancelled-ended-live", "CANCELLED", "live", 1, "ended", "", false, false, "execution closed"},
		{"cancelled-open-elsewhere", "CANCELLED", "stale", 1, "open", "live", true, true, "another live executor still runs it"},
		{"cancelled-norow-live", "CANCELLED", "live", 1, "", "", true, true, "started untracked on a live executor"},
		{"cancelled-restarted-id", "CANCELLED", "live", 1, "ended", "stale", false, false, "tracked elsewhere; executor_id moved to a live caller that re-started the ID"},
		{"cancelled-norow-unstarted", "CANCELLED", "live", 0, "", "", false, false, "cancelled before it ever started"},
		{"cancelled-norow-tomb", "CANCELLED", "tomb", 1, "", "", false, false, "untracked, but its executor shut down"},
		{"cancelled-norow-nolease", "CANCELLED", "unknown", 1, "", "", false, false, "no lease: executor long gone"},
		{"error-norow-live", "ERROR", "live", 1, "", "", false, false, "ERROR is written by the run's own goroutine after its code returned"},
		{"dead-lettered", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "live", 3, "", "", false, false, "final, and not cancelled"},
	}
	var ids []string
	for _, c := range cases {
		run(c.id, c.status, c.executor, c.attempts)
		if c.row != "" {
			on := c.executor
			if c.rowOn != "" {
				on = c.rowOn
			}
			execution(c.id, on, c.row == "open")
		}
		ids = append(ids, p+c.id)
	}
	ids = append(ids, ids[0]) // a duplicate is reported once

	got, err := unsettledRuns(ctx, poolQuery(pool), ids)
	if err != nil {
		t.Fatalf("unsettledRuns: %v", err)
	}
	byID := map[string]UnsettledRun{}
	for _, r := range got {
		if _, dup := byID[r.ID]; dup {
			t.Errorf("%s reported twice", r.ID)
		}
		byID[r.ID] = r
	}
	for _, c := range cases {
		r, unsettled := byID[p+c.id]
		if unsettled != c.wantUnsettled || r.Executing != c.wantExecuting {
			t.Errorf("%s: unsettled=%v executing=%v, want %v/%v (%s)", c.id, unsettled, r.Executing, c.wantUnsettled, c.wantExecuting, c.why)
		}
	}

	if _, err := unsettledRuns(ctx, poolQuery(pool), []string{p + "pending", p + "missing"}); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("unknown ID: err = %v, want ErrRunNotFound", err)
	}
	if got, err := unsettledRuns(ctx, poolQuery(pool), nil); err != nil || got != nil {
		t.Errorf("no IDs: got %v, %v; want nothing", got, err)
	}
}

// TestUnsettledClientMatchesEngine: the Client runs the same core.
func TestUnsettledClientMatchesEngine(t *testing.T) {
	var wait ctxWaitStep
	var wf *PipelineWorkflow[int, int]
	a := newTrackedApp(t, "exec-client-A", func(a *App) { wf = Register(a, "exec-client", Pipe1(Step("wait", wait.run))) })
	defer a.Close(5 * time.Second)
	c, err := NewClient(context.Background(), ClientConfig{DatabaseURL: wpURL(), ApplicationName: "duro-exec-" + t.Name()})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer c.Shutdown(5 * time.Second)

	h, err := wf.Start(a, 1)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitUntil(t, 5*time.Second, "the stage to start", func() bool { return wait.started.Load() == 1 })
	runs, err := c.Unsettled(h.ID())
	if err != nil || len(runs) != 1 || !runs[0].Executing {
		t.Fatalf("client while running: %+v, %v; want one executing run", runs, err)
	}
	if err := c.Cancel(h.ID()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitUntil(t, 3*time.Second, "the client to see the run settle", func() bool {
		runs, err := c.Unsettled(h.ID())
		return err == nil && len(runs) == 0
	})
	if _, err := c.Unsettled("exec-client-no-such-run"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("unknown ID: err = %v, want ErrRunNotFound", err)
	}
}

// TestUnsettledRequiresWorkerPool: an App that tracks nothing refuses rather
// than answering "settled" for everything.
func TestUnsettledRequiresWorkerPool(t *testing.T) {
	a := newMaintenanceApp(t, "exec-no-wp")
	if _, err := a.Unsettled(context.Background(), "anything"); !errors.Is(err, errUnsettledNeedsWorkerPool) {
		t.Errorf("err = %v, want errUnsettledNeedsWorkerPool", err)
	}
}

// TestTrackerRetriesFailedEnds: a close that failed to write is retried by
// the poll, and a retry never closes a newer execution of the same run.
func TestTrackerRetriesFailedEnds(t *testing.T) {
	ctx := context.Background()
	pool := wpConnFor(t)
	wp := &workerPool{pool: pool, logger: wpLogger(), staleThreshold: time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DELETE FROM dbos.duro_run_executions WHERE executor_id = 'exec-retry'") })

	tr := &executionTracker{pool: pool, executorID: "exec-retry", logger: wpLogger(),
		running: map[string]*trackedExecution{}, unended: map[string]string{}}
	for _, id := range []string{"run-a", "run-b"} {
		if err := tr.recordStart(ctx, id, "old"); err != nil {
			t.Fatal(err)
		}
	}
	// run-b started again here under a new token after its failed close.
	if err := tr.recordStart(ctx, "run-b", "new"); err != nil {
		t.Fatal(err)
	}
	tr.unended["run-a"] = "old"
	tr.unended["run-b"] = "old"

	tr.poll(ctx)

	open := openExecutions(t, pool, "exec-retry")
	if want := []string{"run-b"}; len(open) != 1 || open[0] != want[0] {
		t.Errorf("open executions = %v, want %v (run-a closed by the retry; run-b's newer execution untouched)", open, want)
	}
	if len(tr.unended) != 0 {
		t.Errorf("unended = %v, want empty after a successful retry", tr.unended)
	}
}

func openExecutions(t *testing.T, pool *pgxpool.Pool, executor string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT workflow_uuid FROM dbos.duro_run_executions WHERE executor_id = $1 AND ended_at IS NULL`, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// TestReusedExecutorIDClosesOrphanedRows: a process restarting under its
// predecessor's executor ID closes the rows that predecessor left open, or
// its newly live lease would keep counting them forever.
func TestReusedExecutorIDClosesOrphanedRows(t *testing.T) {
	ctx := context.Background()
	pool := wpConnFor(t)
	const executor = "exec-reused-id"
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.duro_run_executions WHERE executor_id = $1", executor)
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id = $1", executor)
	})
	wp := &workerPool{pool: pool, logger: wpLogger(), appName: "exec-reused", version: "v1",
		executorID: executor, nonce: randomHex(), staleThreshold: time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	wp.tracker = &executionTracker{pool: pool, executorID: executor, logger: wpLogger(),
		running: map[string]*trackedExecution{}, unended: map[string]string{}}
	if err := wp.tracker.recordStart(ctx, "left-open-by-a-crash", "t"); err != nil {
		t.Fatal(err)
	}
	if err := wp.firstBeat(ctx); err != nil {
		t.Fatalf("firstBeat: %v", err)
	}
	if open := openExecutions(t, pool, executor); len(open) != 0 {
		t.Errorf("open executions after the restart's claim = %v, want none", open)
	}
}

// TestPruneLeasesDeletesExecutionRows: a long-dead executor's execution rows
// go with its lease; a live executor's stay.
func TestPruneLeasesDeletesExecutionRows(t *testing.T) {
	ctx := context.Background()
	pool := wpConnFor(t)
	const app = "exec-prune-app"
	wp := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: "v1",
		staleThreshold: time.Minute, leasePruneAge: time.Hour}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.duro_run_executions WHERE executor_id LIKE 'exec-prune-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'exec-prune-%'")
	})
	seedHeartbeat(t, pool, "exec-prune-gone", app, "v1", time.Now().Add(-2*time.Hour))
	seedHeartbeat(t, pool, "exec-prune-live", app, "v1", time.Now())
	for _, executor := range []string{"exec-prune-gone", "exec-prune-live"} {
		if _, err := pool.Exec(ctx, `INSERT INTO dbos.duro_run_executions
			(executor_id, workflow_uuid, token, started_at) VALUES ($1, 'run', 't', now())`, executor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := wp.pruneLeases(ctx); err != nil {
		t.Fatalf("pruneLeases: %v", err)
	}
	if open := openExecutions(t, pool, "exec-prune-gone"); len(open) != 0 {
		t.Errorf("dead executor's rows = %v, want pruned", open)
	}
	if open := openExecutions(t, pool, "exec-prune-live"); len(open) != 1 {
		t.Errorf("live executor's rows = %v, want kept", open)
	}
}
