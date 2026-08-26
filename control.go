package duro

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// ErrRunTerminal is returned by Cancel and Resume when the run has already
// reached a final state the operation cannot change: any terminal state for
// Cancel; success or error for Resume, which only revives cancelled and
// retries-exceeded runs. Re-run a finished pipeline with ForkFromStage.
var ErrRunTerminal = errors.New("duro: run is already in a final state")

// ErrRunActive is returned by Resume when the run is still live — pending,
// enqueued, or delayed. Resuming a live run would re-enqueue it while an
// executor may still be running it, a double execution; Cancel it first.
var ErrRunActive = errors.New("duro: run is still active")

// ErrRunInFlight is returned by Resume for a run that was cancelled after it
// had started executing. Cancellation lands at the next stage boundary, so
// the stage in flight keeps running on its executor until it finishes — and
// nothing DBOS records says when that is. Resuming under the same ID would
// re-enqueue the run while that stage may still be running, and would let the
// old executor carry on past it: a double execution. Re-run such a run under
// a new ID with ForkFromStage, which leaves this one cancelled.
var ErrRunInFlight = errors.New("duro: run was cancelled mid-execution and its in-flight stage may still be running")

// execFunc runs one SQL statement against the system database and reports
// the rows it affected. It is how Resume's guarded transition reaches the
// database without this file depending on a driver: the App and the Client
// each build one from their own pool (poolExec).
type execFunc func(ctx context.Context, sql string, args ...any) (int64, error)

// Cancel stops a live run: an enqueued or delayed run never starts, and a
// pending one stops at its next stage boundary (the stage in flight finishes
// — keep stages idempotent). The run's queue is cleared and its state becomes
// StateCancelled. Runs already in a final state return ErrRunTerminal, so a
// cancel that changed nothing is never mistaken for one that did; unknown IDs
// return ErrRunNotFound.
//
// Client.Cancel is the same operation from an enqueue-only process.
func Cancel(ctx Context, workflowID string) error {
	return cancelRun(engineStore(unwrapContext(ctx), nil), workflowID)
}

// cancelRun is the core behind Cancel and Client.Cancel.
func cancelRun(store runStore, workflowID string) error {
	status, err := statusOne(store, "Cancel", workflowID)
	if err != nil {
		return err
	}
	if status.State.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrRunTerminal, workflowID, status.State)
	}
	if err := store.cancel(workflowID); err != nil {
		return fmt.Errorf("duro: cancelling %s: %w", workflowID, notFoundOr(err, workflowID))
	}
	return nil
}

// resumeSQL is Resume's transition: the state guard and the re-enqueue in one
// statement, so two concurrent Resumes — or a Resume racing a worker that has
// already picked the run up — cannot both apply. A check-then-act on DBOS's
// own resume would: its guard admits PENDING, so the second caller would
// re-enqueue a run the first caller's worker is executing.
//
// The guard admits exactly the runs no executor can be executing: a
// dead-lettered run (DBOS assigns MAX_RECOVERY_ATTEMPTS_EXCEEDED while
// re-starting a run, so nothing else holds it) and a cancelled run that never
// started (recovery_attempts is 0 until the first start; a run cancelled while
// executing keeps its count, and its in-flight stage may still be running —
// see ErrRunInFlight). It writes what DBOS's resume writes, except that a run
// which still records its queue stays on it (DBOS moves every resumed run to
// the internal queue, outside its queue's limits). Resetting
// recovery_attempts is intended — an operator's explicit Resume grants a fresh
// budget, as DBOS's does — unlike worker-pool takeover, which must preserve
// the count (see workerpool.go).
//
// Like takeover, it is coupled to DBOS's workflow_status schema, pinned to the
// dbos module version; a column rename fails loudly here rather than
// corrupting anything silently.
const resumeSQL = `UPDATE dbos.workflow_status
	SET status = $2, queue_name = COALESCE(NULLIF(queue_name, ''), $3), recovery_attempts = 0,
	    workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
	    started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
	WHERE workflow_uuid = $1
	  AND (status = $5 OR (status = $6 AND recovery_attempts = 0))`

// resumeTransition applies resumeSQL and reports whether the run was in a
// resumable state and is now enqueued.
func resumeTransition(ctx context.Context, exec execFunc, workflowID string) (bool, error) {
	n, err := exec(ctx, resumeSQL,
		workflowID,
		string(dbos.WorkflowStatusEnqueued),
		internalQueueName,
		time.Now().UnixMilli(),
		string(dbos.WorkflowStatusMaxRecoveryAttemptsExceeded),
		string(dbos.WorkflowStatusCancelled),
	)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Resume revives a run under its original ID: it is re-enqueued and picked up
// by an executor on its application version, where completed stages replay
// from their checkpoints and execution continues from the first stage that
// never finished. The recovery-attempt count starts over. A run that still
// records its queue returns to it; because DBOS clears a run's queue when it
// cancels or dead-letters it, in practice the resumed run executes on DBOS's
// internal queue, outside any concurrency or rate limit of the queue it was
// originally enqueued on.
//
// Exactly two kinds of run resume, because they are the ones no executor can
// still be executing: a run that exceeded its recovery attempts, and a run
// cancelled before it ever started (while enqueued or delayed). A run
// cancelled after it started returns ErrRunInFlight — its in-flight stage may
// still be running and DBOS records nothing that says when it stops — and is
// re-run under a new ID with ForkFromStage instead. Success and error runs
// are finished and return ErrRunTerminal rather than a silent no-op; fork
// those too. Pending, enqueued, and delayed runs return ErrRunActive: Cancel
// first. Unknown IDs return ErrRunNotFound. The state check is part of the
// transition itself — one guarded UPDATE — so concurrent Resumes cannot both
// apply.
//
// The call is bounded by ctx's deadline, or by DefaultReadTimeout when it has
// none, and ends early if ctx is cancelled. It is a method on App rather than
// a function of a Context because the transition is duro's own SQL statement,
// run on the App's database connection. Client.Resume is the same operation
// from an enqueue-only process.
func (a *App) Resume(ctx context.Context, workflowID string) error {
	if ctx == nil {
		return errors.New("duro: Resume requires a non-nil context")
	}
	exec, err := a.sqlExec()
	if err != nil {
		return err
	}
	dctx, done := boundContext(a.DBOSContext, ctx, DefaultReadTimeout)
	defer done()
	return resumeRun(engineStore(dctx, exec), workflowID)
}

// resumeRun is the core behind App.Resume and Client.Resume: the guarded
// transition first, and a status read only to explain why it did not apply.
func resumeRun(store runStore, workflowID string) error {
	if workflowID == "" {
		return errors.New("duro: Resume requires a workflow ID")
	}
	resumed, err := store.resume(workflowID)
	if err != nil {
		return fmt.Errorf("duro: resuming %s: %w", workflowID, err)
	}
	if resumed {
		return nil
	}
	status, err := statusOne(store, "Resume", workflowID)
	if err != nil {
		return err
	}
	switch {
	case status.State == StateSuccess || status.State == StateError:
		return fmt.Errorf("%w: %s is %s; use ForkFromStage to re-run a finished pipeline", ErrRunTerminal, workflowID, status.State)
	case status.State == StateCancelled && status.Attempts > 0:
		return fmt.Errorf("%w: %s was cancelled on executor %s after %d start(s); re-run it under a new ID with ForkFromStage",
			ErrRunInFlight, workflowID, status.ExecutorID, status.Attempts)
	case status.State == StateCancelled || status.State == StateRetriesExceeded:
		// Resumable now but not when the transition ran: it was resumed and
		// cancelled (or dead-lettered) again in between. Report, don't loop.
		return fmt.Errorf("duro: resuming %s: its state changed concurrently (now %s); retry", workflowID, status.State)
	default:
		return fmt.Errorf("%w: %s is %s; Cancel it first", ErrRunActive, workflowID, status.State)
	}
}
