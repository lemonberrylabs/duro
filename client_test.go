package duro_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lemonberrylabs/duro"
)

// r3Queue is the queue the enqueue-only client hands work to and the worker
// listens on.
var r3Queue = duro.NewQueue("r3-queue")

// r3Echo is the job both sides share: the worker registers it, the client
// enqueues it. r3EchoUnregistered names a pipeline nobody registers — the
// typo case, which now has to be written deliberately.
var (
	r3Echo             = duro.NewJob[int, int]("r3-echo")
	r3EchoUnregistered = duro.NewJob[int, int]("r3-echo-null")
	r3Fail             = duro.NewJob[int, int]("r3-fail") // always fails, with a message naming its input
)

func r3EchoPipeline() duro.Pipeline[int, int] {
	return duro.Pipe1(duro.Step("r3-echo-step", func(_ context.Context, in int) (int, error) {
		return in + 1000, nil
	}))
}

func r3FailPipeline() duro.Pipeline[int, int] {
	return duro.Pipe1(duro.Step("r3-fail-step", func(_ context.Context, in int) (int, error) {
		return 0, fmt.Errorf("r3: refusing %d", in)
	}))
}

// launchR3Worker builds a normal (engine) worker that registers the queue and
// the echo pipeline and launches its queue runners. A distinct application
// version keeps the shared TestMain app — which polls every database-backed
// queue — from dequeuing these runs (its dequeue is version-scoped).
func launchR3Worker(t *testing.T, name, version string) *duro.App {
	t.Helper()
	t.Setenv("DBOS__APPVERSION", "")
	w, err := duro.New(context.Background(), duro.Config{
		Name:               name,
		DatabaseURL:        testDatabaseURL(),
		Logger:             quietLogger(),
		ApplicationVersion: version,
	})
	if err != nil {
		t.Fatalf("worker New: %v", err)
	}
	if err := duro.RegisterQueues(w, r3Queue); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	duro.RegisterJob(w, r3Echo, r3EchoPipeline())
	duro.RegisterJob(w, r3Fail, r3FailPipeline())
	if err := w.Launch(); err != nil {
		t.Fatalf("worker Launch: %v", err)
	}
	return w
}

func newR3Client(t *testing.T) *duro.Client {
	t.Helper()
	c, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL: testDatabaseURL(),
		Logger:      quietLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Shutdown(5 * time.Second) })
	return c
}

// TestClientEnqueueAndStatus is the R3 acceptance path: a client with no engine
// enqueues a pipeline by name, a worker executes it, and client and engine
// report the run's status identically. The run is version-pinned so only this
// test's worker runs it.
func TestClientEnqueueAndStatus(t *testing.T) {
	worker := launchR3Worker(t, "duro-r3-worker", "r3-exec-v1")
	defer worker.Shutdown(5 * time.Second)
	client := newR3Client(t)

	h, err := duro.Enqueue(client, r3Queue, r3Echo, 42,
		duro.WithClientApplicationVersion("r3-exec-v1"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	runID := h.ID()

	res, err := h.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if res != 1042 {
		t.Errorf("result = %d, want 1042", res)
	}

	// Status parity: the client and the engine map the same run identically.
	cs, err := client.Status(runID)
	if err != nil {
		t.Fatalf("client.Status: %v", err)
	}
	es, err := duro.Status(worker, runID)
	if err != nil {
		t.Fatalf("duro.Status: %v", err)
	}
	if cs.State != duro.StateSuccess || es.State != duro.StateSuccess {
		t.Errorf("state: client=%s engine=%s, want both success", cs.State, es.State)
	}
	if cs.ID != es.ID || cs.Name != es.Name {
		t.Errorf("status fields diverge: client=%+v engine=%+v", cs, es)
	}
}

// TestClientEnqueueDefaultVersionIsNull proves the request's default: with no
// version option the enqueued run carries a NULL application version, so any
// worker version may run it (rather than dbos's default of the client's own
// version, which no worker would share).
func TestClientEnqueueDefaultVersionIsNull(t *testing.T) {
	client := newR3Client(t)
	conn := wpConn(t)

	h, err := duro.Enqueue(client, r3Queue, r3EchoUnregistered, 7,
		duro.WithClientWorkflowID("r3-null-version-run"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	defer conn.Exec(context.Background(), "DELETE FROM dbos.workflow_status WHERE workflow_uuid=$1", h.ID())

	var version *string
	if err := conn.QueryRow(context.Background(),
		"SELECT application_version FROM dbos.workflow_status WHERE workflow_uuid=$1", h.ID()).Scan(&version); err != nil {
		t.Fatalf("query version: %v", err)
	}
	if version != nil {
		t.Errorf("application_version = %q, want NULL (any-version dequeue)", *version)
	}
}

// TestClientVersionPinningExcludes proves WithClientApplicationVersion restricts
// execution: a run pinned to a version no worker has stays ENQUEUED.
func TestClientVersionPinningExcludes(t *testing.T) {
	worker := launchR3Worker(t, "duro-r3-worker-pin", "r3-real-v")
	defer worker.Shutdown(5 * time.Second)
	client := newR3Client(t)
	conn := wpConn(t)

	h, err := duro.Enqueue(client, r3Queue, r3Echo, 1,
		duro.WithClientApplicationVersion("r3-version-nobody-has"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	defer conn.Exec(context.Background(), "DELETE FROM dbos.workflow_status WHERE workflow_uuid=$1", h.ID())

	// No executor is on that version, so across many poll cycles it is never run.
	time.Sleep(1500 * time.Millisecond)
	s, err := client.Status(h.ID())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if s.State != duro.StateEnqueued {
		t.Errorf("state = %s, want still enqueued (no worker on its pinned version)", s.State)
	}
}

// TestClientStatusUnknownID proves unknown IDs are reported consistently: Status
// returns ErrRunNotFound and StatusAll omits them.
func TestClientStatusUnknownID(t *testing.T) {
	client := newR3Client(t)

	_, err := client.Status("r3-does-not-exist")
	if !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Status error = %v, want ErrRunNotFound", err)
	}
	all, err := client.StatusAll("r3-does-not-exist", "r3-also-missing")
	if err != nil {
		t.Fatalf("StatusAll: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("StatusAll returned %d statuses for unknown IDs, want 0", len(all))
	}
}

// TestClientListRunsAndStepsParity proves the admin reads exist on the client
// and go through the engine's mapping: the same listing, the same fields, the
// same steps, from a process that registers nothing.
func TestClientListRunsAndStepsParity(t *testing.T) {
	worker := launchR3Worker(t, "duro-r3-worker-list", "r3-list-v1")
	defer worker.Shutdown(5 * time.Second)
	client := newR3Client(t)

	var ids []string
	for _, in := range []int{42, 43} {
		time.Sleep(5 * time.Millisecond)
		h, err := duro.Enqueue(client, r3Queue, r3Echo, in, duro.WithClientApplicationVersion("r3-list-v1"))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if _, err := h.Result(); err != nil {
			t.Fatalf("Result: %v", err)
		}
		ids = append(ids, h.ID())
	}

	opts := []duro.ListOption{duro.WithNames(r3Echo.Name()), duro.WithIDs(ids...), duro.WithNewestFirst(), duro.WithInput()}
	fromClient, err := client.ListRuns(opts...)
	if err != nil {
		t.Fatalf("client.ListRuns: %v", err)
	}
	fromEngine, err := duro.ListRuns(worker, opts...)
	if err != nil {
		t.Fatalf("duro.ListRuns: %v", err)
	}
	if len(fromClient) != 2 || len(fromEngine) != 2 {
		t.Fatalf("client/engine listed %d/%d runs, want 2/2", len(fromClient), len(fromEngine))
	}
	wantInput := map[string]string{ids[0]: "42", ids[1]: "43"}
	for i := range fromClient {
		c, e := fromClient[i], fromEngine[i]
		if c.ID != e.ID || c.State != e.State || c.Name != e.Name || c.QueueName != e.QueueName ||
			c.ExecutorID != e.ExecutorID || c.Attempts != e.Attempts || c.Input != e.Input {
			t.Errorf("run %d diverges: client=%+v engine=%+v", i, c, e)
		}
		if c.QueueName != r3Queue.Name() || c.State != duro.StateSuccess || c.Input != wantInput[c.ID] {
			t.Errorf("client run = %+v, want a successful run on %s with input %s", c, r3Queue.Name(), wantInput[c.ID])
		}
		if c.StartedAt.IsZero() || c.StartedAt.After(c.CompletedAt) || !c.StartedAt.Equal(e.StartedAt) {
			t.Errorf("queued run %s StartedAt = %v (engine %v), completed %v; want the dequeue time on both sides", c.ID, c.StartedAt, e.StartedAt, c.CompletedAt)
		}
	}
	if fromClient[0].ID != ids[1] {
		t.Errorf("newest first: got %s, want %s", fromClient[0].ID, ids[1])
	}

	cs, err := client.Steps(ids[0])
	if err != nil {
		t.Fatalf("client.Steps: %v", err)
	}
	es, err := duro.Steps(worker, ids[0])
	if err != nil {
		t.Fatalf("duro.Steps: %v", err)
	}
	if len(cs) != 2 || len(es) != 2 || cs[0].Name != duro.ShapeStepName || cs[1].Name != "r3-echo-step" {
		t.Fatalf("steps: client=%+v engine=%+v, want [shape r3-echo-step] from both", cs, es)
	}
	for i := range cs {
		if cs[i] != es[i] {
			t.Errorf("step %d diverges: client=%+v engine=%+v", i, cs[i], es[i])
		}
	}
}

// TestClientCancelAndResume proves the remediation pair on the client, with
// the same state rules as the engine: an enqueued run is active until
// cancelled, a cancelled run resumes (back to enqueued — no worker on its
// pinned version ever runs it), and unknown IDs are ErrRunNotFound.
func TestClientCancelAndResume(t *testing.T) {
	client := newR3Client(t)
	h, err := duro.Enqueue(client, r3Queue, r3EchoUnregistered, 1,
		duro.WithClientWorkflowID("r3-control-run"),
		duro.WithClientApplicationVersion("r3-version-nobody-has"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	id := h.ID()

	if steps, err := client.Steps(id); err != nil || len(steps) != 0 {
		t.Errorf("Steps(never started) = %+v, %v; want none and no error", steps, err)
	}
	if err := client.Resume(id); !errors.Is(err, duro.ErrRunActive) {
		t.Errorf("Resume(enqueued) = %v, want ErrRunActive", err)
	}

	if err := client.Cancel(id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if s, err := client.Status(id); err != nil || s.State != duro.StateCancelled {
		t.Fatalf("status after Cancel = %+v, %v; want cancelled", s, err)
	}
	if err := client.Cancel(id); !errors.Is(err, duro.ErrRunTerminal) {
		t.Errorf("second Cancel = %v, want ErrRunTerminal", err)
	}

	if err := client.Resume(id); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if s, err := client.Status(id); err != nil || s.State != duro.StateEnqueued {
		t.Errorf("status after Resume = %+v, %v; want enqueued again", s, err)
	}
	if err := client.Cancel(id); err != nil {
		t.Fatalf("cleanup Cancel: %v", err)
	}

	for name, err := range map[string]error{
		"Cancel": client.Cancel("r3-does-not-exist"),
		"Resume": client.Resume("r3-does-not-exist"),
	} {
		if !errors.Is(err, duro.ErrRunNotFound) {
			t.Errorf("%s(unknown) = %v, want ErrRunNotFound", name, err)
		}
	}
	if _, err := client.Steps("r3-does-not-exist"); !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Steps(unknown) = %v, want ErrRunNotFound", err)
	}
}

// TestClientFailedRunExposesError pins the client-side failure reason. A
// dbos.Client context is never launched, and DBOS's default is to skip the
// output/error columns until Launch — so unless the failure-reason query asks
// for them explicitly, an api tier reports every failed run as the placeholder
// "duro: run error" while the workers see the real message. Status, StatusAll,
// ListRuns, and Steps must all carry it from the client.
func TestClientFailedRunExposesError(t *testing.T) {
	worker := launchR3Worker(t, "duro-r3-worker-fail", "r3-fail-v1")
	defer worker.Shutdown(5 * time.Second)
	client := newR3Client(t)

	h, err := duro.Enqueue(client, r3Queue, r3Fail, 9, duro.WithClientApplicationVersion("r3-fail-v1"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := h.Result(); err == nil {
		t.Fatal("expected the run to fail")
	}
	id := h.ID()

	cs, err := client.Status(id)
	if err != nil {
		t.Fatalf("client.Status: %v", err)
	}
	es, err := duro.Status(worker, id)
	if err != nil {
		t.Fatalf("duro.Status: %v", err)
	}
	if cs.State != duro.StateError || es.State != duro.StateError {
		t.Fatalf("state: client=%s engine=%s, want both error", cs.State, es.State)
	}
	if cs.Err == nil || !strings.Contains(cs.Err.Error(), "refusing 9") {
		t.Errorf("client Err = %v, want the recorded failure message", cs.Err)
	}
	if es.Err == nil || cs.Err == nil || cs.Err.Error() != es.Err.Error() {
		t.Errorf("failure reason diverges: client=%v engine=%v", cs.Err, es.Err)
	}

	listed, err := client.ListRuns(duro.WithIDs(id))
	if err != nil {
		t.Fatalf("client.ListRuns: %v", err)
	}
	if len(listed) != 1 || listed[0].Err == nil || listed[0].Err.Error() != es.Err.Error() {
		t.Errorf("client.ListRuns Err = %+v, want the same recorded failure", listed)
	}
	steps, err := client.Steps(id)
	if err != nil {
		t.Fatalf("client.Steps: %v", err)
	}
	if len(steps) != 2 || steps[1].Name != "r3-fail-step" || steps[1].Err == nil || !strings.Contains(steps[1].Err.Error(), "refusing 9") {
		t.Errorf("client.Steps = %+v, want [shape r3-fail-step(err refusing 9)]", steps)
	}
}

// TestClientReadTimeoutBoundsBlockedRead proves a client read cannot outlive
// ReadTimeout: with the workflow table locked so every read blocks, Status
// returns an error at the deadline instead of parking until the lock lifts —
// and the client is usable again once it does.
func TestClientReadTimeoutBoundsBlockedRead(t *testing.T) {
	id := startAndAwait(t, 9)
	c, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL: testDatabaseURL(),
		Logger:      quietLogger(),
		ReadTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Shutdown(5 * time.Second) })
	if s, err := c.Status(id); err != nil || s.State != duro.StateSuccess {
		t.Fatalf("healthy Status = %+v, %v; want success", s, err)
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

	start := time.Now()
	_, err = c.Status(id)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Status succeeded while the table was locked; want a timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timed-out Status error = %v, want one wrapping context.DeadlineExceeded", err)
	}
	if elapsed < 250*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("blocked Status returned after %v, want about the 300ms ReadTimeout", elapsed)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	if s, err := c.Status(id); err != nil || s.State != duro.StateSuccess {
		t.Errorf("Status after the lock lifted = %+v, %v; want the client usable again", s, err)
	}
}

// TestClientWithContextBoundsReads proves a WithContext view observes the
// caller's context on every read path — a cancelled request fails fast on
// Status and Steps alike — while the root client and a live view are
// unaffected and agree with each other.
func TestClientWithContextBoundsReads(t *testing.T) {
	id := startAndAwait(t, 10)
	client := newR3Client(t)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	dead := client.WithContext(cancelled)
	start := time.Now()
	if _, err := dead.Status(id); err == nil {
		t.Error("Status on a cancelled context succeeded, want an error")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("Status error = %v, want one wrapping context.Canceled", err)
	}
	if _, err := dead.Steps(id); err == nil {
		t.Error("Steps on a cancelled context succeeded, want an error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancelled reads took %v, want a prompt return", elapsed)
	}

	root, err := client.Status(id)
	if err != nil {
		t.Fatalf("root Status after a cancelled view: %v", err)
	}
	live, err := client.WithContext(context.Background()).Status(id)
	if err != nil {
		t.Fatalf("live view Status: %v", err)
	}
	if root.ID != live.ID || root.State != live.State || root.State != duro.StateSuccess {
		t.Errorf("root/view diverge: %+v vs %+v", root, live)
	}

	defer func() {
		if recover() == nil {
			t.Error("WithContext(nil) did not panic")
		}
	}()
	client.WithContext(nil) //nolint:staticcheck // the nil is the point
}

// TestClientReadTimeoutValidation pins the default and the refusal of a
// negative timeout at construction.
func TestClientReadTimeoutValidation(t *testing.T) {
	if duro.DefaultReadTimeout != 30*time.Second {
		t.Errorf("DefaultReadTimeout = %v, want 30s", duro.DefaultReadTimeout)
	}
	_, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL: testDatabaseURL(),
		Logger:      quietLogger(),
		ReadTimeout: -time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "ReadTimeout") {
		t.Errorf("NewClient(ReadTimeout<0) = %v, want a ReadTimeout error", err)
	}
}

// TestClientWithContextDeadlineIsDeadline proves a caller's deadline is
// reported as one: with the table locked and a request context that expires
// well before ReadTimeout, the read fails at the request deadline with an
// error wrapping context.DeadlineExceeded — not context.Canceled — so HTTP
// timeout classification sees the right cause.
func TestClientWithContextDeadlineIsDeadline(t *testing.T) {
	id := startAndAwait(t, 11)
	client := newR3Client(t) // default 30s ReadTimeout; the request deadline must win

	conn := wpConn(t)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE dbos.workflow_status IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("locking: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = client.WithContext(ctx).Status(id)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Status succeeded while the table was locked; want a deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want one wrapping context.DeadlineExceeded and not context.Canceled", err)
	}
	if elapsed < 150*time.Millisecond || elapsed > 5*time.Second {
		t.Errorf("returned after %v, want about the 200ms request deadline", elapsed)
	}
}
