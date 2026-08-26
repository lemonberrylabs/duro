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

// TestStateRoundTrip pins the State→DBOS mapping ListRuns filters with as the
// exact inverse of the DBOS→State mapping statuses are reported with, so a
// filter on a State always matches the runs reported in that State — unknown
// values included, which pass through both ways.
func TestStateRoundTrip(t *testing.T) {
	for _, s := range []State{StatePending, StateEnqueued, StateDelayed, StateSuccess, StateError, StateCancelled, StateRetriesExceeded, State("SOMETHING_NEW")} {
		if got := stateOf(dbosStatusOf(s)); got != s {
			t.Errorf("stateOf(dbosStatusOf(%s)) = %s", s, got)
		}
	}
	for _, d := range []dbos.WorkflowStatusType{dbos.WorkflowStatusPending, dbos.WorkflowStatusEnqueued, dbos.WorkflowStatusDelayed,
		dbos.WorkflowStatusSuccess, dbos.WorkflowStatusError, dbos.WorkflowStatusCancelled, dbos.WorkflowStatusMaxRecoveryAttemptsExceeded} {
		if got := dbosStatusOf(stateOf(d)); got != d {
			t.Errorf("dbosStatusOf(stateOf(%s)) = %s", d, got)
		}
	}
	if dbosStatusOf(StateRetriesExceeded) != dbos.WorkflowStatusMaxRecoveryAttemptsExceeded {
		t.Error("retries_exceeded must filter as MAX_RECOVERY_ATTEMPTS_EXCEEDED")
	}
}

// TestInputJSON pins how a loaded input becomes RunStatus.Input: DBOS's
// decoded JSON text passes through verbatim, nil stays nil, and anything else
// is marshalled.
func TestInputJSON(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"42", "42"},
		{`{"a":[1,2]}`, `{"a":[1,2]}`},
		{[]byte(`"text"`), `"text"`},
		{"not json", `"not json"`},
		{struct{ N int }{3}, `{"N":3}`},
	}
	for _, c := range cases {
		got, err := inputJSON(c.in)
		if err != nil {
			t.Errorf("inputJSON(%v): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("inputJSON(%v) = %s, want %s", c.in, got, c.want)
		}
	}
	if _, err := inputJSON(make(chan int)); err == nil {
		t.Error("inputJSON(unmarshallable) = nil error, want one")
	}
}

// TestListRunsShortCircuits proves an empty membership filter and an invalid
// option are resolved before any query is issued.
func TestListRunsShortCircuits(t *testing.T) {
	store := runStore{list: func(...dbos.ListWorkflowsOption) ([]dbos.WorkflowStatus, error) {
		t.Fatal("ListRuns queried the store")
		return nil, nil
	}}
	runs, err := listRunsWith(store, []ListOption{WithNames("a"), WithIDs()})
	if err != nil || runs == nil || len(runs) != 0 {
		t.Errorf("empty filter: runs=%v err=%v, want an empty non-nil list", runs, err)
	}
	if _, err := listRunsWith(store, []ListOption{WithLimit(-5), WithLimit(-6)}); err == nil || err.Error() != "duro: ListRuns: WithLimit(-5): limit must be positive" {
		t.Errorf("invalid limit error = %v, want the first invalid option reported", err)
	}
}
