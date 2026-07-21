package duro_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// TestStatusByID proves the check-on-read path: a persisted workflow ID is
// enough to reconcile a run's state from any process — no Handle needed.
func TestStatusByID(t *testing.T) {
	handle, err := registeredWf.Start(app, 7)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	if _, err := handle.Result(); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	status, err := duro.Status(app, handle.ID())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != duro.StateSuccess || !status.State.Terminal() || status.State.Failed() {
		t.Errorf("state = %s (terminal=%v failed=%v), want success/terminal/not-failed",
			status.State, status.State.Terminal(), status.State.Failed())
	}
	if status.ID != handle.ID() || status.Name != "registeredPipeline" {
		t.Errorf("ID/Name = %s/%s, want %s/registeredPipeline", status.ID, status.Name, handle.ID())
	}
	if status.Err != nil {
		t.Errorf("Err = %v, want nil for a successful run", status.Err)
	}
	if status.CreatedAt.IsZero() || status.CompletedAt.IsZero() {
		t.Errorf("timestamps = created %v / completed %v, want both set", status.CreatedAt, status.CompletedAt)
	}
}

// TestStatusFailedRunExposesError proves the recorded failure surfaces on
// RunStatus.Err for error-state runs.
func TestStatusFailedRunExposesError(t *testing.T) {
	handle, err := dbos.RunWorkflow(dctx, retryPredicateWorkflow, "permanent")
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	wfID := handle.GetWorkflowID()
	if _, err := handle.GetResult(); err == nil {
		t.Fatal("expected the run to fail")
	}

	status, err := duro.Status(app, wfID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != duro.StateError || !status.State.Failed() {
		t.Errorf("state = %s, want error/failed", status.State)
	}
	if status.Err == nil || !strings.Contains(status.Err.Error(), "permanent") {
		t.Errorf("Err = %v, want the recorded permanent failure", status.Err)
	}
	if status.CompletedAt.IsZero() {
		t.Errorf("CompletedAt is zero, want set for a terminal run")
	}
}

// TestStatusCancelledRun proves cancellation maps to StateCancelled with a
// non-nil Err even though DBOS records no error for it.
func TestStatusCancelledRun(t *testing.T) {
	const wfID = "status-cancelled-run"
	if _, err := dbos.RunWorkflow(dctx, recvGreetingWorkflow, "", dbos.WithWorkflowID(wfID)); err != nil {
		t.Fatalf("starting parked run: %v", err)
	}
	if err := dbos.CancelWorkflow(dctx, wfID); err != nil {
		t.Fatalf("cancelling: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := duro.Status(app, wfID)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if status.State.Terminal() {
			if status.State != duro.StateCancelled {
				t.Errorf("state = %s, want cancelled", status.State)
			}
			if status.Err == nil {
				t.Errorf("Err = nil, want the synthesized cancellation error")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never reached a terminal state (last: %s)", status.State)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStatusUnknownID proves the empty-result ambiguity is resolved into
// ErrRunNotFound.
func TestStatusUnknownID(t *testing.T) {
	_, err := duro.Status(app, "no-such-run-anywhere")
	if !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("error = %v, want ErrRunNotFound", err)
	}
}

// TestStatusAll proves the batch form returns found runs in request order
// and omits unknown IDs.
func TestStatusAll(t *testing.T) {
	h1, err := registeredWf.Start(app, 1)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	h2, err := registeredWf.Start(app, 2)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	if _, err := h1.Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Result(); err != nil {
		t.Fatal(err)
	}

	statuses, err := duro.StatusAll(app, h2.ID(), "missing-run", h1.ID())
	if err != nil {
		t.Fatalf("StatusAll: %v", err)
	}
	if len(statuses) != 2 || statuses[0].ID != h2.ID() || statuses[1].ID != h1.ID() {
		t.Errorf("statuses = %+v, want [%s %s] in request order", statuses, h2.ID(), h1.ID())
	}
}

// TestAttach proves a process can reconnect to a run by ID and await its
// result — the restarted-process path.
func TestAttach(t *testing.T) {
	started, err := registeredWf.Start(app, 20)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	if _, err := started.Result(); err != nil {
		t.Fatal(err)
	}

	attached, err := duro.Attach[int](app, started.ID())
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	result, err := attached.Result()
	if err != nil {
		t.Fatalf("attached Result: %v", err)
	}
	if result != 41 {
		t.Errorf("result = %d, want 41", result)
	}

	if _, err := duro.Attach[int](app, "no-such-run-anywhere"); !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Attach unknown ID error = %v, want ErrRunNotFound", err)
	}
}
