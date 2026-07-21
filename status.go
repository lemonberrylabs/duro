package duro

import (
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// State is a run's lifecycle state.
type State string

const (
	StatePending         State = "pending"  // running or ready to run
	StateEnqueued        State = "enqueued" // waiting on a queue
	StateDelayed         State = "delayed"  // waiting for its start delay
	StateSuccess         State = "success"
	StateError           State = "error" // completed with an error
	StateCancelled       State = "cancelled"
	StateRetriesExceeded State = "retries_exceeded" // exceeded max recovery attempts
)

// Terminal reports whether the run has reached a final state.
func (s State) Terminal() bool {
	switch s {
	case StateSuccess, StateError, StateCancelled, StateRetriesExceeded:
		return true
	}
	return false
}

// Failed reports whether the run finished without succeeding.
func (s State) Failed() bool { return s.Terminal() && s != StateSuccess }

// RunStatus is the cheap status view of a run: no input or output payloads
// are loaded or deserialized, making it safe for polling paths.
type RunStatus struct {
	ID    string
	Name  string // registered workflow (pipeline) name
	State State
	// Err is the recorded failure for failed runs — nil otherwise. Cancelled
	// and retries-exceeded runs that recorded no error get a synthesized one,
	// so Err is always non-nil when State.Failed().
	Err                error
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        time.Time // zero until terminal
	ApplicationVersion string
	ForkedFrom         string // original run's ID when this run was forked
}

// ErrRunNotFound is returned by Status and Attach for an unknown workflow ID.
var ErrRunNotFound = errors.New("duro: run not found")

// Status fetches a run's current status by workflow ID — the reconcile
// primitive for consumers that persist run IDs and check on them later. It
// works from any process attached to the same system database; no Handle
// needed.
func Status(ctx Context, workflowID string) (RunStatus, error) {
	if workflowID == "" {
		return RunStatus{}, errors.New("duro: Status requires a workflow ID")
	}
	statuses, err := StatusAll(ctx, workflowID)
	if err != nil {
		return RunStatus{}, err
	}
	if len(statuses) == 0 {
		return RunStatus{}, fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
	}
	return statuses[0], nil
}

// StatusAll is the batch form of Status: it returns the status of every
// listed run that exists, in the requested order, silently omitting unknown
// IDs (compare lengths to detect them).
//
// The common case is one payload-free query. DBOS stores a run's failure
// message alongside its output, so when the batch contains failed runs their
// recorded errors are fetched in a second query scoped to just those runs —
// healthy polling stays cheap, failure reasons still surface.
func StatusAll(ctx Context, workflowIDs ...string) ([]RunStatus, error) {
	if len(workflowIDs) == 0 {
		return nil, nil
	}
	dctx := unwrapContext(ctx)
	found, err := dbos.ListWorkflows(dctx,
		dbos.WithWorkflowIDs(workflowIDs),
		dbos.WithLoadInput(false),
		dbos.WithLoadOutput(false),
	)
	if err != nil {
		return nil, fmt.Errorf("duro: fetching run status: %w", err)
	}

	byID := make(map[string]RunStatus, len(found))
	var failedIDs []string
	for _, s := range found {
		status := runStatusOf(s)
		byID[s.ID] = status
		if status.State.Failed() {
			failedIDs = append(failedIDs, s.ID)
		}
	}
	if len(failedIDs) > 0 {
		failed, err := dbos.ListWorkflows(dctx,
			dbos.WithWorkflowIDs(failedIDs),
			dbos.WithLoadInput(false), // loadOutput stays on: it carries the recorded error
		)
		if err != nil {
			return nil, fmt.Errorf("duro: fetching failure reasons: %w", err)
		}
		for _, s := range failed {
			if s.Error != nil {
				status := byID[s.ID]
				status.Err = s.Error
				byID[s.ID] = status
			}
		}
	}

	out := make([]RunStatus, 0, len(found))
	for _, id := range workflowIDs {
		if s, ok := byID[id]; ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// Attach reconnects to an existing run by workflow ID and returns its
// handle — how a restarted process awaits a result instead of just polling
// Status. R must match the workflow's result type.
func Attach[R any](ctx Context, workflowID string) (Handle[R], error) {
	h, err := dbos.RetrieveWorkflow[R](unwrapContext(ctx), workflowID)
	if err != nil {
		if errors.Is(err, &dbos.DBOSError{Code: dbos.NonExistentWorkflowError}) {
			return Handle[R]{}, fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
		}
		return Handle[R]{}, err
	}
	return newHandle(h, nil)
}

// runStatusOf converts a DBOS status record into duro's view of it.
func runStatusOf(s dbos.WorkflowStatus) RunStatus {
	state := stateOf(s.Status)
	err := s.Error
	if err == nil && state.Failed() {
		err = errors.New("duro: run " + string(state))
	}
	return RunStatus{
		ID:                 s.ID,
		Name:               s.Name,
		State:              state,
		Err:                err,
		CreatedAt:          s.CreatedAt,
		UpdatedAt:          s.UpdatedAt,
		CompletedAt:        s.CompletedAt,
		ApplicationVersion: s.ApplicationVersion,
		ForkedFrom:         s.ForkedFrom,
	}
}

// stateOf maps DBOS lifecycle statuses onto duro states; an unrecognized
// status (a future DBOS addition) passes through as-is rather than failing.
func stateOf(s dbos.WorkflowStatusType) State {
	switch s {
	case dbos.WorkflowStatusPending:
		return StatePending
	case dbos.WorkflowStatusEnqueued:
		return StateEnqueued
	case dbos.WorkflowStatusDelayed:
		return StateDelayed
	case dbos.WorkflowStatusSuccess:
		return StateSuccess
	case dbos.WorkflowStatusError:
		return StateError
	case dbos.WorkflowStatusCancelled:
		return StateCancelled
	case dbos.WorkflowStatusMaxRecoveryAttemptsExceeded:
		return StateRetriesExceeded
	}
	return State(s)
}
