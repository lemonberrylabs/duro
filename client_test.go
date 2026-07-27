package duro_test

import (
	"context"
	"errors"
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
)

func r3EchoPipeline() duro.Pipeline[int, int] {
	return duro.Pipe1(duro.Step("r3-echo-step", func(_ context.Context, in int) (int, error) {
		return in + 1000, nil
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
