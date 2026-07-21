package duro

import (
	"errors"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// TestStateMapping pins the DBOS→duro state mapping and the Terminal/Failed
// classification, including the pass-through for unknown future statuses.
func TestStateMapping(t *testing.T) {
	cases := []struct {
		dbosStatus       dbos.WorkflowStatusType
		want             State
		terminal, failed bool
	}{
		{dbos.WorkflowStatusPending, StatePending, false, false},
		{dbos.WorkflowStatusEnqueued, StateEnqueued, false, false},
		{dbos.WorkflowStatusDelayed, StateDelayed, false, false},
		{dbos.WorkflowStatusSuccess, StateSuccess, true, false},
		{dbos.WorkflowStatusError, StateError, true, true},
		{dbos.WorkflowStatusCancelled, StateCancelled, true, true},
		{dbos.WorkflowStatusMaxRecoveryAttemptsExceeded, StateRetriesExceeded, true, true},
		{dbos.WorkflowStatusType("SOMETHING_NEW"), State("SOMETHING_NEW"), false, false},
	}
	for _, c := range cases {
		got := stateOf(c.dbosStatus)
		if got != c.want {
			t.Errorf("stateOf(%s) = %s, want %s", c.dbosStatus, got, c.want)
		}
		if got.Terminal() != c.terminal || got.Failed() != c.failed {
			t.Errorf("%s: Terminal/Failed = %v/%v, want %v/%v", got, got.Terminal(), got.Failed(), c.terminal, c.failed)
		}
	}
}

// TestRunStatusSynthesizesFailureError proves Err is always non-nil for
// failed runs, even when DBOS recorded no error (cancelled runs).
func TestRunStatusSynthesizesFailureError(t *testing.T) {
	cancelled := runStatusOf(dbos.WorkflowStatus{ID: "x", Status: dbos.WorkflowStatusCancelled})
	if cancelled.Err == nil || cancelled.Err.Error() != "duro: run cancelled" {
		t.Errorf("cancelled Err = %v, want the synthesized cancellation error", cancelled.Err)
	}

	recorded := errors.New("boom")
	failed := runStatusOf(dbos.WorkflowStatus{ID: "x", Status: dbos.WorkflowStatusError, Error: recorded})
	if failed.Err != recorded {
		t.Errorf("failed Err = %v, want the recorded error", failed.Err)
	}

	ok := runStatusOf(dbos.WorkflowStatus{ID: "x", Status: dbos.WorkflowStatusSuccess})
	if ok.Err != nil {
		t.Errorf("success Err = %v, want nil", ok.Err)
	}
}
