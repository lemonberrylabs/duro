package duro

import (
	"encoding/json"
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
// are loaded or deserialized, making it safe for polling paths. The one
// exception is Input, which ListRuns fills only when asked with WithInput.
type RunStatus struct {
	ID    string
	Name  string // registered workflow (pipeline) name
	State State
	// Err is the recorded failure for failed runs — nil otherwise. Cancelled
	// and retries-exceeded runs that recorded no error get a synthesized one,
	// so Err is always non-nil when State.Failed().
	Err       error
	CreatedAt time.Time
	UpdatedAt time.Time
	// StartedAt is when a queued run was dequeued to start. DBOS records no
	// start time for a run started directly on its executor, so it is zero
	// for those — and while a queued run is still enqueued or delayed.
	StartedAt          time.Time
	CompletedAt        time.Time // zero until terminal
	ApplicationName    string    // owning DBOS application; empty while unclaimed
	ApplicationVersion string
	ExecutorID         string // the executor that last ran (or is running) it
	// Attempts counts how many times execution has been started — 1 for a run
	// that completed without recovery, more after crashes or takeovers, 0
	// while still waiting on a queue.
	Attempts int
	// QueueName is the queue the run was enqueued on; "" for a run started
	// directly on its executor. DBOS clears it when a run is cancelled or
	// exceeds its recovery attempts.
	QueueName string
	// ParentID is the run that started this one — set for FanOut children and
	// other child workflows, "" for top-level runs.
	ParentID   string
	ForkedFrom string // original run's ID when this run was forked
	// ScheduleName identifies the database-backed schedule that created this
	// run. It is empty for manually started and client-enqueued workflows.
	ScheduleName string
	// Input is the run's input as JSON text — json.RawMessage(status.Input)
	// re-emits it. It is empty unless the run was listed with WithInput:
	// loading payloads is what the status path otherwise avoids. It is the
	// stored JSON, not a decoded value, so it is available from an
	// enqueue-only Client that registers no workflow types. (A string rather
	// than a byte slice keeps RunStatus comparable.)
	Input string
}

// ErrRunNotFound is returned by Status, Attach, Steps, Cancel, and Resume for
// an unknown workflow ID.
var ErrRunNotFound = errors.New("duro: run not found")

// Status fetches a run's current status by workflow ID — the reconcile
// primitive for consumers that persist run IDs and check on them later. It
// works from any process attached to the same system database; no Handle
// needed.
func Status(ctx Context, workflowID string) (RunStatus, error) {
	return statusOne(engineStore(unwrapContext(ctx), nil), "Status", workflowID)
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
	return statusAll(engineStore(unwrapContext(ctx), nil), workflowIDs)
}

// runStore is the set of DBOS operations the run inspection and remediation
// APIs — Status, StatusAll, ListRuns, Steps, Cancel, Resume — need from their
// data source. The engine builds one from its DBOS context (engineStore) and
// the Client from its dbos.Client (Client.store), and every public entry point
// on either side is a thin call into the same core function over this struct.
// That is what keeps the two from ever disagreeing on how a DBOS status maps
// to a State, on what "terminal" means, or on which runs Resume accepts.
type runStore struct {
	list   func(...dbos.ListWorkflowsOption) ([]dbos.WorkflowStatus, error)
	steps  func(workflowID string) ([]dbos.StepInfo, error)
	cancel func(workflowID string) error
	// resume applies the guarded transition (resumeSQL) and reports whether
	// the run was resumable and is now enqueued.
	resume func(workflowID string) (bool, error)
}

// engineStore adapts a DBOS context — and, for Resume, a way to execute SQL
// on the same database — to a runStore. exec may be nil when the caller
// never resumes.
func engineStore(dctx Context, exec execFunc) runStore {
	return runStore{
		list: func(opts ...dbos.ListWorkflowsOption) ([]dbos.WorkflowStatus, error) {
			return dbos.ListWorkflows(dctx, opts...)
		},
		steps: func(workflowID string) ([]dbos.StepInfo, error) {
			return dbos.GetWorkflowSteps(dctx, workflowID, dbos.WithStepsLoadOutput(false))
		},
		cancel: func(workflowID string) error {
			return dbos.CancelWorkflow(dctx, workflowID)
		},
		resume: func(workflowID string) (bool, error) {
			if exec == nil {
				return false, errors.New("duro: Resume needs a database connection: call it on the App or a Client")
			}
			return resumeTransition(dctx, exec, workflowID)
		},
	}
}

// statusOne is the core behind Status and Client.Status: the single-ID form
// of statusAll with the empty-result ambiguity resolved into ErrRunNotFound.
func statusOne(store runStore, what, workflowID string) (RunStatus, error) {
	if workflowID == "" {
		return RunStatus{}, fmt.Errorf("duro: %s requires a workflow ID", what)
	}
	statuses, err := statusAll(store, []string{workflowID})
	if err != nil {
		return RunStatus{}, err
	}
	if len(statuses) == 0 {
		return RunStatus{}, fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
	}
	return statuses[0], nil
}

// statusAll is the core behind StatusAll and Client.StatusAll: listRuns scoped
// to the given IDs, reordered to match the request.
func statusAll(store runStore, workflowIDs []string) ([]RunStatus, error) {
	if len(workflowIDs) == 0 {
		return nil, nil
	}
	found, err := listRuns(store, false, dbos.WithFilterWorkflowIDs(workflowIDs...))
	if err != nil {
		return nil, err
	}
	byID := make(map[string]RunStatus, len(found))
	for _, s := range found {
		byID[s.ID] = s
	}
	out := make([]RunStatus, 0, len(found))
	for _, id := range workflowIDs {
		if s, ok := byID[id]; ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// listRuns is the shared query-and-map core behind StatusAll and ListRuns (and
// their Client twins). filters are the DBOS filters to apply; loadInput adds
// each run's stored input payload to its RunStatus.
//
// The common case is one payload-free query. DBOS stores a run's failure
// message alongside its output, so when the result contains failed runs their
// recorded errors are fetched in a second query scoped to just those runs —
// healthy listing stays cheap, failure reasons still surface.
func listRuns(store runStore, loadInput bool, filters ...dbos.ListWorkflowsOption) ([]RunStatus, error) {
	opts := make([]dbos.ListWorkflowsOption, 0, len(filters)+2)
	opts = append(opts, filters...)
	opts = append(opts, dbos.WithFilterLoadInput(loadInput), dbos.WithFilterLoadOutput(false))
	found, err := store.list(opts...)
	if err != nil {
		return nil, fmt.Errorf("duro: fetching run status: %w", err)
	}

	out := make([]RunStatus, 0, len(found))
	index := make(map[string]int, len(found))
	var failedIDs []string
	for _, s := range found {
		status := runStatusOf(s)
		if loadInput {
			input, err := inputJSON(s.Input)
			if err != nil {
				return nil, fmt.Errorf("duro: run %s: %w", s.ID, err)
			}
			status.Input = input
		}
		index[s.ID] = len(out)
		out = append(out, status)
		if status.State.Failed() {
			failedIDs = append(failedIDs, s.ID)
		}
	}
	if len(failedIDs) > 0 {
		// The recorded error lives in the output columns, and DBOS only scans
		// those when asked. Ask explicitly: its default is "on once launched",
		// which is true on an engine and never true on a dbos.Client — left
		// implicit, an api tier would report every failure as the placeholder
		// while the workers see the real message.
		failed, err := store.list(
			dbos.WithFilterWorkflowIDs(failedIDs...),
			dbos.WithFilterLoadInput(false),
			dbos.WithFilterLoadOutput(true),
		)
		if err != nil {
			return nil, fmt.Errorf("duro: fetching failure reasons: %w", err)
		}
		for _, s := range failed {
			if i, ok := index[s.ID]; ok && s.Error != nil {
				out[i].Err = s.Error
			}
		}
	}
	return out, nil
}

// inputJSON renders the input payload DBOS loaded for a run as JSON text.
// duro registers no custom serializer, so DBOS hands back the stored JSON —
// base64-decoded for its default format, verbatim for portable runs — as a
// string. Anything else (a value decoded by a custom serializer) is
// marshalled.
func inputJSON(v any) (string, error) {
	switch in := v.(type) {
	case nil:
		return "", nil
	case string:
		if json.Valid([]byte(in)) {
			return in, nil
		}
	case []byte:
		if json.Valid(in) {
			return string(in), nil
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("encoding input as JSON: %w", err)
	}
	return string(b), nil
}

// AttachJob is Attach with the result type taken from a Job rather than
// written out at the call site, so it cannot drift from the registration. Use
// it whenever the run belongs to a job you declared.
func AttachJob[P, R any](ctx Context, job Job[P, R], workflowID string) (Handle[R], error) {
	mustValidJob("AttachJob", job)
	return Attach[R](ctx, workflowID)
}

// Attach reconnects to an existing run by workflow ID and returns its
// handle — how a restarted process awaits a result instead of just polling
// Status. R must match the workflow's result type; prefer AttachJob, which
// takes it from the job's declaration instead.
func Attach[R any](ctx Context, workflowID string) (Handle[R], error) {
	h, err := dbos.RetrieveWorkflow[R](unwrapContext(ctx), workflowID)
	if err != nil {
		return Handle[R]{}, notFoundOr(err, workflowID)
	}
	return newHandle(h, nil)
}

// notFoundOr maps DBOS's unknown-workflow error onto ErrRunNotFound and
// passes every other error through unchanged.
func notFoundOr(err error, workflowID string) error {
	if errors.Is(err, dbos.ErrNonExistentWorkflow) {
		return fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
	}
	return err
}

// runStatusOf converts a DBOS status record into duro's view of it. It never
// carries the input payload: Input is set only by listRuns, only on request.
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
		StartedAt:          s.StartedAt,
		CompletedAt:        s.CompletedAt,
		ApplicationName:    s.ApplicationName,
		ApplicationVersion: s.ApplicationVersion,
		ExecutorID:         s.ExecutorID,
		Attempts:           s.Attempts,
		QueueName:          s.QueueName,
		ParentID:           s.ParentWorkflowID,
		ForkedFrom:         s.ForkedFrom,
		ScheduleName:       s.ScheduleName,
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

// dbosStatusOf is the inverse of stateOf, for filtering by State: every duro
// state maps back to the DBOS status it came from, and an unrecognized value
// passes through as-is so a future DBOS status can still be filtered on.
func dbosStatusOf(s State) dbos.WorkflowStatusType {
	switch s {
	case StatePending:
		return dbos.WorkflowStatusPending
	case StateEnqueued:
		return dbos.WorkflowStatusEnqueued
	case StateDelayed:
		return dbos.WorkflowStatusDelayed
	case StateSuccess:
		return dbos.WorkflowStatusSuccess
	case StateError:
		return dbos.WorkflowStatusError
	case StateCancelled:
		return dbos.WorkflowStatusCancelled
	case StateRetriesExceeded:
		return dbos.WorkflowStatusMaxRecoveryAttemptsExceeded
	}
	return dbos.WorkflowStatusType(s)
}
