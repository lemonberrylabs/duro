package duro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
)

// wpIVersion / wpIName isolate the integration-test fleet from both the shared
// TestMain app and the black-box worker-pool tests (distinct app name and
// application version → version-scoped dequeue and app-scoped sweeps never
// cross).
const (
	wpIName    = "duro-wp-itest"
	wpIVersion = "wp-itest-v1"
)

// The blocking pipeline: wp-pre → wp-block → wp-post. wp-block ignores its
// context and parks on wpBlockCh while wpBlocking is set, so a run sits PENDING
// (and undrainable) until the test releases it — the crash target. The atomic
// counters distinguish "step body ran" from "checkpoint replayed", exactly like
// the fork-replay tests.
var (
	wpPreCount   atomic.Int64
	wpBlockCount atomic.Int64
	wpPostCount  atomic.Int64
	wpBlocking   atomic.Bool
	wpBlockMu    sync.Mutex
	wpBlockCh    chan struct{}
)

func wpResetBlocker() {
	wpPreCount.Store(0)
	wpBlockCount.Store(0)
	wpPostCount.Store(0)
	wpBlocking.Store(true)
	wpBlockMu.Lock()
	wpBlockCh = make(chan struct{})
	wpBlockMu.Unlock()
}

func wpReleaseBlocker() {
	wpBlocking.Store(false)
	wpBlockMu.Lock()
	select {
	case <-wpBlockCh: // already closed
	default:
		close(wpBlockCh)
	}
	wpBlockMu.Unlock()
}

func wpBlockStep(_ context.Context, in int) (int, error) {
	wpBlockCount.Add(1)
	if wpBlocking.Load() {
		wpBlockMu.Lock()
		ch := wpBlockCh
		wpBlockMu.Unlock()
		<-ch // ignores ctx: a step that does not check cancellation keeps the run PENDING
	}
	return in * 2, nil
}

func wpBlockingPipeline() Pipeline[int, int] {
	return Pipe3(
		Step("wp-pre", func(_ context.Context, in int) (int, error) { wpPreCount.Add(1); return in + 1, nil }),
		Step("wp-block", wpBlockStep),
		Step("wp-post", func(_ context.Context, in int) (int, error) { wpPostCount.Add(1); return in + 100, nil }),
	)
}

// newWPWorker builds and launches a worker-pool App with fast cadences and the
// blocking pipeline registered under a name shared across the fleet.
func newWPWorker(t *testing.T, executorID, version string, opts ...WorkerPoolOption) (*App, *PipelineWorkflow[int, int]) {
	t.Helper()
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	base := []WorkerPoolOption{
		WithHeartbeatInterval(150 * time.Millisecond),
		WithStaleThreshold(1 * time.Second),
	}
	a, err := New(context.Background(), Config{
		// Per-test app name scopes the heartbeat sweep, so one test's crashed
		// executors and stranded runs are invisible to another test's sweeper
		// even when they share an application version.
		Name:               wpIName + "-" + t.Name(),
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ExecutorID:         executorID,
		ApplicationVersion: version,
	}, WithWorkerPool(append(base, opts...)...), WithSweepInterval(150*time.Millisecond))
	if err != nil {
		t.Fatalf("New worker %s: %v", executorID, err)
	}
	wf := RegisterJob(a, wpBlockingJob, wpBlockingPipeline())
	if err := a.Launch(); err != nil {
		t.Fatalf("Launch worker %s: %v", executorID, err)
	}
	return a, wf
}

// The fan-out combination pipeline: expand → FanOut(square) → reduce(sum) →
// block. wpChildCount counts child executions so takeover can be shown to replay
// the children's checkpoints, not re-run them.
var (
	wpChildCount atomic.Int64
	wpFanBlocked atomic.Int64
	wpFanQueue   = NewQueue("wp-fan-queue")
)

func wpFanChild(_ dbos.DBOSContext, n int) (int, error) {
	wpChildCount.Add(1)
	return n * n, nil
}

func wpFanBlockStep(_ context.Context, sum int) (int, error) {
	wpFanBlocked.Add(1)
	if wpBlocking.Load() {
		wpBlockMu.Lock()
		ch := wpBlockCh
		wpBlockMu.Unlock()
		<-ch
	}
	return sum, nil
}

func wpFanPipeline() Pipeline[int, int] {
	return Pipe4(
		Expand("wp-fan-expand", func(_ context.Context, in int) ([]int, error) {
			out := make([]int, in)
			for i := range out {
				out[i] = i + 1
			}
			return out, nil
		}),
		FanOut("wp-fan", wpFanQueue, Workflow(wpFanChild)),
		Reduce("wp-fan-sum", func(_ context.Context, acc, in int) (int, error) { return acc + in, nil }, 0),
		Step("wp-fan-block", wpFanBlockStep),
	)
}

// newWPFanWorker launches a worker-pool App with the fan-out pipeline (and its
// child workflow, which must be registered by name on every executor so an
// enqueued or recovered child can run anywhere).
func newWPFanWorker(t *testing.T, executorID string) (*App, *PipelineWorkflow[int, int]) {
	t.Helper()
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	a, err := New(context.Background(), Config{
		Name:               wpIName + "-" + t.Name(),
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ExecutorID:         executorID,
		ApplicationVersion: wpIVersion,
	}, WithWorkerPool(
		WithHeartbeatInterval(150*time.Millisecond),
		WithStaleThreshold(1*time.Second),
	), WithSweepInterval(150*time.Millisecond))
	if err != nil {
		t.Fatalf("New fan worker %s: %v", executorID, err)
	}
	dbos.RegisterWorkflow(a.Context(), wpFanChild, dbos.WithWorkflowName("wp-fan-child"))
	wf := Register(a, "wp-fan-parent", wpFanPipeline())
	if err := a.Launch(); err != nil {
		t.Fatalf("Launch fan worker %s: %v", executorID, err)
	}
	return a, wf
}

// crashApp simulates kill -9: it stops the sweeper, stops the heartbeat, and
// stops the DBOS runtime — but does NOT tombstone the lease (a crash cannot).
// The ctx-ignoring block step keeps the run PENDING through DBOS's drain, so the
// run is left owned by a now-dead executor whose lease will simply expire.
//
// It consumes closeOnce so a deferred App.Shutdown on the crashed app is a
// no-op, rather than tombstoning against a closed pool — a state a real crashed
// process never reaches.
func crashApp(a *App) {
	a.wp.stopMaintenance()
	a.wp.closeOnce.Do(func() {
		a.wp.stopBeat()
		dbos.Shutdown(a.DBOSContext, 300*time.Millisecond)
		a.wp.close()
	})
}

// wpClientQueue carries client-enqueued work to the worker fleet.
var wpClientQueue = NewQueue("wp-client-queue")

// wpBlockingJob is the blocking pipeline's cross-process identity, shared by
// the worker registrations and the client enqueue.
var wpBlockingJob = NewJob[int, int]("wp-blocking")

// wpConnFor opens a throwaway pool for inspecting workflow_status directly.
func wpConnFor(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, desc)
}

func waitState(t *testing.T, ctx Context, id string, want State, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, fmt.Sprintf("run %s to reach %s", id, want), func() bool {
		s, err := Status(ctx, id)
		return err == nil && s.State == want
	})
}

// TestTakeoverCrashDirectStarted is AC2 for the default case: a directly-started
// pipeline (started_at NULL). A blocks mid-run, is killed without a graceful
// shutdown, and B's sweeper adopts and completes the run — with the checkpointed
// pre-crash step replayed, not re-run.
func TestTakeoverCrashDirectStarted(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPWorker(t, "wp-crash-A", wpIVersion)
	appB, _ := newWPWorker(t, "wp-crash-B", wpIVersion)
	defer appB.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 5) // direct-started: no queue, started_at is NULL
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()

	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	crashApp(appA)     // kill -9: no tombstone, lease left to expire
	wpReleaseBlocker() // let whichever survivor adopts it run to completion

	waitState(t, appB, runID, StateSuccess, 15*time.Second)

	got, err := Attach[int](appB, runID)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	res, err := got.Result()
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if res != 112 {
		t.Errorf("result = %d, want 112 (5+1=6 ×2=12 +100=112)", res)
	}
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1 (checkpoint replayed on takeover, not re-run)", n)
	}
	if n := wpPostCount.Load(); n != 1 {
		t.Errorf("wp-post executions = %d, want 1 (completed once, on the survivor)", n)
	}
}

// TestTakeoverGracefulShutdownNoStaleWait is AC3: a graceful shutdown tombstones
// the lease, so an undrained run is adopted on the next survivor sweep with no
// stale wait. The stale threshold is set far higher than the test's patience, so
// completing at all proves the tombstone path (a crash would need the full 60s).
func TestTakeoverGracefulShutdownNoStaleWait(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)
	longStale := WithStaleThreshold(60 * time.Second)

	appA, wfA := newWPWorker(t, "wp-graceful-A", wpIVersion, longStale)
	appB, _ := newWPWorker(t, "wp-graceful-B", wpIVersion, longStale)
	defer appB.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 5)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()
	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	appA.Shutdown(300 * time.Millisecond) // graceful: drains (times out), then tombstones the lease
	wpReleaseBlocker()

	// Well under the 60s stale threshold — only the tombstone can explain this.
	waitState(t, appB, runID, StateSuccess, 15*time.Second)
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1 (checkpoint replayed)", n)
	}
}

// TestTakeoverVersionSkewNotAdopted is AC4: a run started on application version
// v1 is never taken over by a v2 sweeper — a new deploy must not adopt (and
// replay on incompatible code) the previous version's in-flight runs.
func TestTakeoverVersionSkewNotAdopted(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPWorker(t, "wp-skew-A", "wp-itest-v1")
	appB, _ := newWPWorker(t, "wp-skew-B", "wp-itest-v2") // different version
	defer appB.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 5)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()
	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	crashApp(appA)

	// Give the v2 sweeper ample time (well past the stale threshold and many sweeps).
	time.Sleep(2 * time.Second)
	s, err := Status(appB, runID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if s.State != StatePending {
		t.Errorf("run state = %s, want still PENDING (stranded on its own version)", s.State)
	}
	if n := wpBlockCount.Load(); n != 1 {
		t.Errorf("block step ran %d times; the v2 fleet replayed a v1 run (want 1)", n)
	}
}

// TestTakeoverFreshExecutorNotTaken proves the sweeper leaves a live executor's
// PENDING run alone: takeover is liveness-gated, so a merely-running run is never
// stolen from under its owner.
func TestTakeoverFreshExecutorNotTaken(t *testing.T) {
	wpResetBlocker()

	appA, wfA := newWPWorker(t, "wp-fresh-A", wpIVersion)
	defer appA.Shutdown(5 * time.Second)
	appB, _ := newWPWorker(t, "wp-fresh-B", wpIVersion)
	defer appB.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 5)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()
	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	// A stays alive and heartbeating; across several sweeps B must not adopt.
	time.Sleep(1500 * time.Millisecond)
	if n := wpBlockCount.Load(); n != 1 {
		t.Errorf("block step ran %d times; B adopted a live executor's run (want 1)", n)
	}
	if s, _ := Status(appB, runID); s.State != StatePending {
		t.Errorf("run state = %s, want still PENDING under its live owner", s.State)
	}

	// Released, the live owner finishes it itself.
	wpReleaseBlocker()
	waitState(t, appA, runID, StateSuccess, 10*time.Second)
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1", n)
	}
	if n := wpPostCount.Load(); n != 1 {
		t.Errorf("wp-post executions = %d, want 1", n)
	}
}

// TestTakeoverConcurrentSweepersExactlyOnce is the yank regression: two live
// survivors sweep the same dead executor concurrently. The atomic, ownership-
// checked UPDATE (unlike raw ResumeWorkflows) must ensure the run is adopted and
// completed exactly once.
func TestTakeoverConcurrentSweepersExactlyOnce(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPWorker(t, "wp-conc-A", wpIVersion)
	appB, _ := newWPWorker(t, "wp-conc-B", wpIVersion)
	defer appB.Shutdown(5 * time.Second)
	appC, _ := newWPWorker(t, "wp-conc-C", wpIVersion)
	defer appC.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 5)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()
	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	crashApp(appA)
	wpReleaseBlocker()

	waitState(t, appB, runID, StateSuccess, 15*time.Second)
	if n := wpPostCount.Load(); n != 1 {
		t.Errorf("wp-post executions = %d, want exactly 1 despite two concurrent sweepers (yank regression)", n)
	}
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1", n)
	}
}

// newWPPoisonWorker launches a worker whose pipeline is registered with a
// recovery-attempt limit, so repeated takeovers eventually exhaust it.
func newWPPoisonWorker(t *testing.T, executorID string) (*App, *PipelineWorkflow[int, int]) {
	t.Helper()
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	a, err := New(context.Background(), Config{
		Name:               wpIName + "-" + t.Name(),
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ExecutorID:         executorID,
		ApplicationVersion: wpIVersion,
	}, WithWorkerPool(
		WithHeartbeatInterval(150*time.Millisecond),
		WithStaleThreshold(1*time.Second),
	), WithSweepInterval(150*time.Millisecond))
	if err != nil {
		t.Fatalf("New poison worker %s: %v", executorID, err)
	}
	// maxRetries=1 means DBOS quarantines on the third start attempt.
	wf := Register(a, "wp-poison", wpBlockingPipeline(), dbos.WithMaxRetries(1))
	if err := a.Launch(); err != nil {
		t.Fatalf("Launch poison worker %s: %v", executorID, err)
	}
	return a, wf
}

// TestTakeoverQuarantinesPoisonRun closes the loop on the recovery-attempt
// counter: a run that kills every process that touches it must eventually stop
// being adopted. Each takeover re-enqueues it, each start increments
// recovery_attempts, and past the limit DBOS parks it in
// MAX_RECOVERY_ATTEMPTS_EXCEEDED — which the PENDING-only takeover never picks
// up again. Resetting the counter on adoption (as DBOS's own resume does) would
// make this loop forever, walking the fleet down one worker at a time.
func TestTakeoverQuarantinesPoisonRun(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPPoisonWorker(t, "wp-poison-A")
	appB, _ := newWPPoisonWorker(t, "wp-poison-B")
	appC, _ := newWPPoisonWorker(t, "wp-poison-C")
	live := map[string]*App{"wp-poison-A": appA, "wp-poison-B": appB, "wp-poison-C": appC}
	conn := wpConnFor(t)

	// Any survivor may win a takeover, so the test must always kill whichever
	// worker currently holds the run rather than assume an order.
	var runID string
	crashOwner := func() {
		t.Helper()
		var owner string
		if err := conn.QueryRow(context.Background(),
			"SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid=$1", runID).Scan(&owner); err != nil {
			t.Fatalf("reading owner: %v", err)
		}
		app, ok := live[owner]
		if !ok {
			t.Fatalf("run is owned by %q, which is not a live worker", owner)
		}
		crashApp(app)
		delete(live, owner)
	}
	survivor := func() *App {
		for _, a := range live {
			return a
		}
		t.Fatal("no live workers left")
		return nil
	}
	defer func() {
		for _, a := range live {
			a.Shutdown(5 * time.Second)
		}
	}()

	h, err := wfA.Start(appA, 5)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID = h.ID()

	// Attempt 1, then its owner dies.
	waitUntil(t, 5*time.Second, "the first worker to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })
	crashOwner()

	// A survivor adopts it (attempt 2), then that owner dies too.
	waitUntil(t, 20*time.Second, "a survivor to adopt and reach the block step", func() bool { return wpBlockCount.Load() >= 2 })
	crashOwner()

	// The last survivor adopts it once more; that third start exhausts the
	// limit, so the run is quarantined instead of executing again.
	last := survivor()
	waitUntil(t, 30*time.Second, "the run to be quarantined", func() bool {
		s, err := Status(last, runID)
		return err == nil && s.State == StateRetriesExceeded
	})
	if n := wpBlockCount.Load(); n != 2 {
		t.Errorf("block step ran %d times, want 2 — the quarantined run executed again", n)
	}

	// And it stays quarantined: further sweeps must not resurrect it.
	time.Sleep(time.Second)
	s, err := Status(last, runID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if s.State != StateRetriesExceeded {
		t.Errorf("state = %s after further sweeps, want it to stay retries_exceeded", s.State)
	}
}

// TestTakeoverClientEnqueuedRun covers the path examples/fleet advertises: a run
// enqueued by an engine-less client, executed by a worker, then taken over when
// that worker dies.
//
// It also guards the DBOS behavior takeover quietly depends on for every queued
// run. Takeover matches rows on application_version = this executor's and
// executor_id ∈ dead — both of which a queued run only acquires when DBOS stamps
// them at dequeue. So the test asserts the dequeued row actually carries them:
// if a dbos upgrade stopped stamping either, queued runs would silently become
// unadoptable, and this fails instead.
//
// The enqueue pins the fleet's version rather than using the NULL default. This
// suite's shared app polls every database-backed queue, so it would dequeue a
// NULL-version (any-version) run it has no registration for and strand it. The
// NULL default is covered on its own in TestClientEnqueueDefaultVersionIsNull.
func TestTakeoverClientEnqueuedRun(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, _ := newWPWorker(t, "wp-client-A", wpIVersion)
	appB, _ := newWPWorker(t, "wp-client-B", wpIVersion)
	defer appB.Shutdown(5 * time.Second)

	// Both workers listen on this queue; the client only enqueues.
	if err := RegisterQueues(appA, wpClientQueue); err != nil {
		t.Fatalf("register queue on A: %v", err)
	}
	if err := RegisterQueues(appB, wpClientQueue); err != nil {
		t.Fatalf("register queue on B: %v", err)
	}
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: wpURL(), Logger: wpLogger()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Shutdown(5 * time.Second)

	h, err := Enqueue(client, wpClientQueue, wpBlockingJob, 5,
		WithClientApplicationVersion(wpIVersion))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	runID := h.ID()

	waitUntil(t, 20*time.Second, "a worker to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	// The dequeued row must satisfy both of takeover's predicates.
	conn := wpConnFor(t)
	var version, owner *string
	if err := conn.QueryRow(context.Background(),
		"SELECT application_version, executor_id FROM dbos.workflow_status WHERE workflow_uuid=$1", runID).
		Scan(&version, &owner); err != nil {
		t.Fatalf("reading dequeued row: %v", err)
	}
	if version == nil || *version != wpIVersion {
		t.Fatalf("application_version after dequeue = %v, want %q — takeover's version predicate can no longer match queued runs", version, wpIVersion)
	}
	if owner == nil || (*owner != "wp-client-A" && *owner != "wp-client-B") {
		t.Fatalf("executor_id after dequeue = %v, want the dequeuing worker — takeover's ownership predicate can no longer match queued runs", owner)
	}
	survivor := appB
	if *owner == "wp-client-B" {
		crashApp(appB)
		survivor = appA
	} else {
		crashApp(appA)
	}
	defer survivor.Shutdown(5 * time.Second)
	wpReleaseBlocker()

	waitState(t, survivor, runID, StateSuccess, 20*time.Second)
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1 (checkpoint replayed on takeover)", n)
	}
	if n := wpPostCount.Load(); n != 1 {
		t.Errorf("wp-post executions = %d, want 1 (completed exactly once)", n)
	}
}

// TestTakeoverHeartbeatThroughDrain guards the corrected shutdown ordering: the
// heartbeat keeps beating through DBOS's drain, so a run still executing on a
// draining node is not adopted by a survivor even after the stale threshold
// elapses. (Stopping the heartbeat before the drain would strand it here.)
func TestTakeoverHeartbeatThroughDrain(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPWorker(t, "wp-drain-A", wpIVersion) // stale threshold 1s
	appB, _ := newWPWorker(t, "wp-drain-B", wpIVersion)
	defer appB.Shutdown(5 * time.Second)

	if _, err := wfA.Start(appA, 5); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitUntil(t, 5*time.Second, "A to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	// Graceful shutdown drains in the background; the ctx-ignoring run keeps the
	// drain open past the stale threshold.
	done := make(chan struct{})
	go func() { appA.Shutdown(3 * time.Second); close(done) }()

	// Across a window well past the 1s stale threshold, A beats through its
	// drain, so B must not adopt the still-owned run.
	time.Sleep(1500 * time.Millisecond)
	if n := wpBlockCount.Load(); n != 1 {
		t.Errorf("block step ran %d times while A was still draining and beating; B adopted prematurely (want 1)", n)
	}

	wpReleaseBlocker()
	<-done // A finishes draining and tombstones
}

// wpLimitedQueue carries the queue-preservation case: a concurrency-limited
// queue, so a run re-enqueued anywhere else would execute outside the limit.
var wpLimitedQueue = NewQueue("wp-limited-queue", WithConcurrency(1))

// TestTakeoverReturnsRunToItsQueue is the live-fleet half of
// TestTakeoverPreservesQueue: a pipeline enqueued on a concurrency-limited
// queue, taken over mid-run, must be re-enqueued on that same queue and run to
// completion from there — not shunted onto the internal queue, where it would
// execute outside the limit the queue exists to enforce.
func TestTakeoverReturnsRunToItsQueue(t *testing.T) {
	wpResetBlocker()
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPWorker(t, "wp-queue-A", wpIVersion)
	appB, _ := newWPWorker(t, "wp-queue-B", wpIVersion)
	for _, a := range []*App{appA, appB} {
		if err := RegisterQueues(a, wpLimitedQueue); err != nil {
			t.Fatalf("register queue: %v", err)
		}
	}

	h, err := wfA.Start(appA, 5, dbos.WithQueue(wpLimitedQueue.Name()))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()

	waitUntil(t, 20*time.Second, "a worker to reach the block step", func() bool { return wpBlockCount.Load() >= 1 })

	// Either worker may have dequeued it, so kill whichever owns it.
	conn := wpConnFor(t)
	var owner string
	if err := conn.QueryRow(context.Background(),
		"SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid=$1", runID).Scan(&owner); err != nil {
		t.Fatalf("reading owner: %v", err)
	}
	survivor := appB
	if owner == "wp-queue-B" {
		crashApp(appB)
		survivor = appA
	} else {
		crashApp(appA)
	}
	defer survivor.Shutdown(5 * time.Second)
	wpReleaseBlocker()

	waitState(t, survivor, runID, StateSuccess, 20*time.Second)

	// Completion leaves queue_name alone, so the row still records where the
	// adopted run was dispatched from.
	var queue string
	if err := conn.QueryRow(context.Background(),
		"SELECT queue_name FROM dbos.workflow_status WHERE workflow_uuid=$1", runID).Scan(&queue); err != nil {
		t.Fatalf("reading queue: %v", err)
	}
	if queue != wpLimitedQueue.Name() {
		t.Errorf("queue_name = %q after takeover, want %q — the adopted run left its concurrency-limited queue and ran outside the limit",
			queue, wpLimitedQueue.Name())
	}
	if n := wpPreCount.Load(); n != 1 {
		t.Errorf("wp-pre executions = %d, want 1 (checkpoint replayed on takeover)", n)
	}
	if n := wpPostCount.Load(); n != 1 {
		t.Errorf("wp-post executions = %d, want 1 (completed exactly once)", n)
	}
}

// TestTakeoverFanOutCombination is the CLAUDE.md combination case: a
// direct-started parent that fans out onto a queue is taken over mid-run. The
// survivor must adopt the parent and complete it while replaying the already-
// finished children from their checkpoints — never re-running them.
func TestTakeoverFanOutCombination(t *testing.T) {
	wpResetBlocker()
	wpChildCount.Store(0)
	wpFanBlocked.Store(0)
	t.Cleanup(wpReleaseBlocker)

	appA, wfA := newWPFanWorker(t, "wp-fan-A")
	appB, _ := newWPFanWorker(t, "wp-fan-B")
	defer appB.Shutdown(5 * time.Second)

	h, err := wfA.Start(appA, 4) // items 1..4 → squares 1,4,9,16 → sum 30
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	runID := h.ID()

	// The parent awaits all children before the block step, so all four have run
	// exactly once by the time we crash.
	waitUntil(t, 15*time.Second, "parent to reach the block after fan-out", func() bool { return wpFanBlocked.Load() >= 1 })
	if n := wpChildCount.Load(); n != 4 {
		t.Fatalf("children before crash = %d, want 4", n)
	}

	crashApp(appA)
	wpReleaseBlocker()

	waitState(t, appB, runID, StateSuccess, 20*time.Second)
	got, err := Attach[int](appB, runID)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	res, err := got.Result()
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if res != 30 {
		t.Errorf("result = %d, want 30 (1+4+9+16)", res)
	}
	if n := wpChildCount.Load(); n != 4 {
		t.Errorf("child executions = %d, want 4 — takeover re-ran fan-out children instead of replaying their checkpoints", n)
	}
}

// White-box worker-pool tests. They live in package duro so they can drive the
// takeover UPDATE directly (R2a, against synthetic workflow_status rows) and
// simulate a hard executor crash (R2b) — stopping a process's heartbeat and
// DBOS runtime without the graceful tombstone that App.Shutdown writes.

func wpURL() string {
	if url := os.Getenv("DURO_TEST_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_test", username)
}

func wpLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func seedHeartbeat(t *testing.T, pool *pgxpool.Pool, execID, app, ver string, last time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO dbos.duro_executor_heartbeats (executor_id, app_name, application_version, last_heartbeat)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (executor_id) DO UPDATE SET
		   app_name=EXCLUDED.app_name, application_version=EXCLUDED.application_version, last_heartbeat=EXCLUDED.last_heartbeat`,
		execID, app, ver, last)
	if err != nil {
		t.Fatalf("seed heartbeat %s: %v", execID, err)
	}
}

func seedWorkflow(t *testing.T, pool *pgxpool.Pool, id, status, execID, ver string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO dbos.workflow_status (workflow_uuid, status, executor_id, application_version)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (workflow_uuid) DO UPDATE SET
		   status=EXCLUDED.status, executor_id=EXCLUDED.executor_id, application_version=EXCLUDED.application_version`,
		id, status, execID, ver)
	if err != nil {
		t.Fatalf("seed workflow %s: %v", id, err)
	}
}

// TestTakeoverEligibility drives the takeover UPDATE against a matrix of seeded
// workflow_status rows and asserts exactly which are adopted — the eligibility
// predicate (heartbeat liveness, version, PENDING-only, app scoping) plus
// idempotency, without a running fleet.
func TestTakeoverEligibility(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const app, ver = "r2a-app", "r2a-v1"
	wp := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: ver, staleThreshold: 5 * time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r2a-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'r2a-%'")
	})

	seedHeartbeat(t, pool, "r2a-dead", app, ver, time.Now().Add(-10*time.Minute)) // stale → dead
	seedHeartbeat(t, pool, "r2a-live", app, ver, time.Now())                      // fresh → alive
	seedHeartbeat(t, pool, "r2a-tomb", app, ver, time.Unix(0, 0))                 // tombstone → dead
	seedHeartbeat(t, pool, "r2a-otherapp", "other-app", ver, time.Now().Add(-10*time.Minute))

	cases := []struct {
		id, status, executor, version string
		wantAdopt                     bool
	}{
		{"r2a-dead-pending", "PENDING", "r2a-dead", ver, true},          // stale owner
		{"r2a-live-pending", "PENDING", "r2a-live", ver, false},         // fresh owner
		{"r2a-tomb-pending", "PENDING", "r2a-tomb", ver, true},          // tombstoned owner
		{"r2a-absent-pending", "PENDING", "r2a-absent", ver, false},     // no heartbeat row (unknown)
		{"r2a-version-skew", "PENDING", "r2a-dead", "r2a-v2", false},    // other version
		{"r2a-enqueued", "ENQUEUED", "r2a-dead", ver, false},            // not PENDING
		{"r2a-success", "SUCCESS", "r2a-dead", ver, false},              // terminal
		{"r2a-cancelled", "CANCELLED", "r2a-dead", ver, false},          // terminal
		{"r2a-otherapp-pending", "PENDING", "r2a-otherapp", ver, false}, // dead, but a different app
	}
	want := map[string]bool{}
	for _, c := range cases {
		seedWorkflow(t, pool, c.id, c.status, c.executor, c.version)
		if c.wantAdopt {
			want[c.id] = true
		}
	}

	adopted, err := wp.takeover(ctx, pool)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	got := map[string]bool{}
	for _, id := range adopted {
		got[id] = true
	}
	for _, c := range cases {
		if got[c.id] != c.wantAdopt {
			t.Errorf("run %s: adopted=%v, want %v", c.id, got[c.id], c.wantAdopt)
		}
	}

	// Idempotency: adopted rows are now ENQUEUED, so a second pass re-adopts none.
	again, err := wp.takeover(ctx, pool)
	if err != nil {
		t.Fatalf("second takeover: %v", err)
	}
	for _, id := range again {
		if want[id] {
			t.Errorf("second takeover re-adopted %s — not idempotent", id)
		}
	}
}

// seedQueuedWorkflow seeds a PENDING run with an explicit queue_name — SQL NULL
// when queue is nil — so takeover's queue handling can be driven across all
// three shapes a real row takes.
func seedQueuedWorkflow(t *testing.T, pool *pgxpool.Pool, id, execID, ver string, queue *string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO dbos.workflow_status (workflow_uuid, status, executor_id, application_version, queue_name)
		 VALUES ($1,'PENDING',$2,$3,$4)
		 ON CONFLICT (workflow_uuid) DO UPDATE SET
		   status='PENDING', executor_id=EXCLUDED.executor_id,
		   application_version=EXCLUDED.application_version, queue_name=EXCLUDED.queue_name`,
		id, execID, ver, queue)
	if err != nil {
		t.Fatalf("seed queued workflow %s: %v", id, err)
	}
}

// TestTakeoverPreservesQueue proves an adopted run goes back on the queue it came
// from, and that only a run with no queue falls back to the internal one.
//
// Rewriting every adoption onto the internal queue (as DBOS's resume does) would
// exempt the run from its queue's concurrency and rate limits — silently, and
// only under recovery, which is exactly when nobody is looking.
//
// The empty-string case is the one that bites: DBOS stores the empty string,
// not NULL, for a directly started run, so a plain COALESCE would leave those
// adopted runs ENQUEUED under an empty queue name that no queue runner polls —
// stranded forever by the sweep meant to rescue them.
func TestTakeoverPreservesQueue(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const app, ver = "r2a-queue-app", "r2a-queue-v1"
	wp := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: ver, staleThreshold: 5 * time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r2a-queue-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'r2a-queue-%'")
	})

	seedHeartbeat(t, pool, "r2a-queue-dead", app, ver, time.Now().Add(-10*time.Minute))

	limited, empty, internal := "r2a-queue-limited", "", internalQueueName
	cases := []struct {
		id   string
		seed *string
		want string
		why  string
	}{
		{"r2a-queue-named", &limited, limited, "a queued run must return to its own queue, or it runs outside that queue's limits"},
		{"r2a-queue-empty", &empty, internalQueueName, "a directly started run stores '' and has no queue to return to"},
		{"r2a-queue-null", nil, internalQueueName, "the NULL variant of a run with no queue"},
		{"r2a-queue-internal", &internal, internalQueueName, "a run already on the internal queue stays there"},
	}
	for _, c := range cases {
		seedQueuedWorkflow(t, pool, c.id, "r2a-queue-dead", ver, c.seed)
	}

	adopted, err := wp.takeover(ctx, pool)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	got := map[string]bool{}
	for _, id := range adopted {
		got[id] = true
	}

	for _, c := range cases {
		if !got[c.id] {
			t.Errorf("run %s was not adopted", c.id)
			continue
		}
		var queue, status string
		if err := pool.QueryRow(ctx,
			"SELECT queue_name, status FROM dbos.workflow_status WHERE workflow_uuid=$1", c.id).
			Scan(&queue, &status); err != nil {
			t.Fatalf("reading %s: %v", c.id, err)
		}
		if queue != c.want {
			t.Errorf("run %s: queue_name = %q after takeover, want %q — %s", c.id, queue, c.want, c.why)
		}
		if status != string(dbos.WorkflowStatusEnqueued) {
			t.Errorf("run %s: status = %q, want ENQUEUED", c.id, status)
		}
	}
}

// TestTakeoverPreservesRecoveryAttempts proves takeover leaves recovery_attempts
// alone. DBOS increments it on every start and parks a run in
// MAX_RECOVERY_ATTEMPTS_EXCEEDED past the limit; zeroing it on each adoption
// would disable that circuit breaker, letting a run that kills its host be
// adopted forever, one worker at a time.
func TestTakeoverPreservesRecoveryAttempts(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const app, ver = "r2a-attempts-app", "r2a-attempts-v1"
	wp := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: ver, staleThreshold: 5 * time.Minute}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r2a-attempts-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'r2a-attempts-%'")
	})

	seedHeartbeat(t, pool, "r2a-attempts-dead", app, ver, time.Now().Add(-10*time.Minute))
	seedWorkflow(t, pool, "r2a-attempts-run", "PENDING", "r2a-attempts-dead", ver)
	if _, err := pool.Exec(ctx,
		"UPDATE dbos.workflow_status SET recovery_attempts = 7 WHERE workflow_uuid = 'r2a-attempts-run'"); err != nil {
		t.Fatalf("seeding attempts: %v", err)
	}

	if _, err := wp.takeover(ctx, pool); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	var attempts int64
	if err := pool.QueryRow(ctx,
		"SELECT recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = 'r2a-attempts-run'").Scan(&attempts); err != nil {
		t.Fatalf("reading attempts: %v", err)
	}
	if attempts != 7 {
		t.Errorf("recovery_attempts = %d after takeover, want 7 preserved — resetting it disables DBOS's poison-run circuit breaker", attempts)
	}
}

// TestHeartbeatNonceDetectsDuplicateExecutorID proves a second process claiming
// the same executor ID is detected rather than silently sharing (and
// invalidating) one lease — the failure mode of a DBOS__VMID set per deployment
// instead of per pod.
func TestHeartbeatNonceDetectsDuplicateExecutorID(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const app, ver, id = "r2a-nonce-app", "r2a-nonce-v1", "r2a-nonce-shared"
	first := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: ver, executorID: id, nonce: randomHex()}
	second := &workerPool{pool: pool, logger: wpLogger(), appName: app, version: ver, executorID: id, nonce: randomHex()}
	if err := first.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id = $1", id)
	})

	if err := first.claimLease(ctx); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := first.beat(ctx); err != nil {
		t.Fatalf("first beat: %v", err)
	}

	// The second process claims the same ID (as a duplicated identity would).
	if err := second.claimLease(ctx); err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if err := second.beat(ctx); err != nil {
		t.Errorf("second beat after claiming: %v", err)
	}

	// The first process now discovers it no longer owns the lease.
	if err := first.beat(ctx); !errors.Is(err, errLeaseStolen) {
		t.Errorf("first beat error = %v, want errLeaseStolen", err)
	}

	// And it cannot tombstone the live process's lease on the way out.
	if err := first.tombstone(ctx); err != nil {
		t.Fatalf("first tombstone: %v", err)
	}
	var last time.Time
	if err := pool.QueryRow(ctx,
		"SELECT last_heartbeat FROM dbos.duro_executor_heartbeats WHERE executor_id = $1", id).Scan(&last); err != nil {
		t.Fatalf("reading heartbeat: %v", err)
	}
	if time.Since(last) > time.Minute {
		t.Error("a departing process tombstoned a lease another process owns — that declares a live executor dead")
	}
}

// TestPruneLeases proves long-dead leases are removed, but only when their
// executor has no non-terminal runs left: an absent lease means "unknown
// executor, do not touch", so pruning one with live-looking runs would make
// those runs permanently unadoptable.
func TestPruneLeases(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, wpURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	const app, ver = "r2a-prune-app", "r2a-prune-v1"
	wp := &workerPool{
		pool: pool, logger: wpLogger(), appName: app, version: ver,
		staleThreshold: time.Minute, leasePruneAge: time.Hour,
	}
	if err := wp.ensureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r2a-prune-%'")
		pool.Exec(ctx, "DELETE FROM dbos.duro_executor_heartbeats WHERE executor_id LIKE 'r2a-prune-%'")
	})

	ancient := time.Now().Add(-2 * time.Hour)
	seedHeartbeat(t, pool, "r2a-prune-gone", app, ver, ancient)      // no runs → prune
	seedHeartbeat(t, pool, "r2a-prune-owes", app, ver, ancient)      // owns a PENDING run → keep
	seedHeartbeat(t, pool, "r2a-prune-recent", app, ver, time.Now()) // still alive → keep
	seedWorkflow(t, pool, "r2a-prune-run", "PENDING", "r2a-prune-owes", ver)
	seedWorkflow(t, pool, "r2a-prune-done", "SUCCESS", "r2a-prune-gone", ver) // terminal: no obstacle

	if _, err := wp.pruneLeases(ctx); err != nil {
		t.Fatalf("pruneLeases: %v", err)
	}

	for _, tc := range []struct {
		id       string
		wantKept bool
		why      string
	}{
		{"r2a-prune-gone", false, "long dead with only terminal runs"},
		{"r2a-prune-owes", true, "still owns a PENDING run — pruning would strand it forever"},
		{"r2a-prune-recent", true, "heartbeat is fresh"},
	} {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM dbos.duro_executor_heartbeats WHERE executor_id=$1", tc.id).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tc.id, err)
		}
		if kept := n > 0; kept != tc.wantKept {
			t.Errorf("lease %s kept=%v, want %v (%s)", tc.id, kept, tc.wantKept, tc.why)
		}
	}
}
