package duro

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// TestResumeTransitionGuard pins the atomicity Resume rests on: the state
// check is the UPDATE's own WHERE clause, so only a retries-exceeded row or a
// cancelled row that never started flips to enqueued, and every other state —
// including PENDING, which a concurrent Resume or a worker may have produced
// since the caller last looked, and CANCELLED with a start on record, whose
// in-flight stage may still be running — is left exactly as it was. It also
// pins the queue rule: a recorded queue is kept, an absent or empty one falls
// back to the internal queue.
func TestResumeTransitionGuard(t *testing.T) {
	pool := wpConnFor(t)
	ctx := context.Background()
	exec := poolExec(pool)
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'resume-guard-%'")
	})

	cases := []struct {
		status   string
		attempts int
		want     bool
	}{
		{"PENDING", 1, false},
		{"ENQUEUED", 0, false},
		{"DELAYED", 0, false},
		{"SUCCESS", 1, false},
		{"ERROR", 1, false},
		{"CANCELLED", 1, false}, // cancelled after a start: its stage may still be running
		{"CANCELLED", 0, true},  // cancelled before any start
		{"MAX_RECOVERY_ATTEMPTS_EXCEEDED", 3, true},
	}
	for _, c := range cases {
		id := fmt.Sprintf("resume-guard-%s-%d", strings.ToLower(c.status), c.attempts)
		seedWorkflow(t, pool, id, c.status, "resume-guard-exec", "resume-guard-v")
		if _, err := pool.Exec(ctx, "UPDATE dbos.workflow_status SET recovery_attempts = $2 WHERE workflow_uuid = $1", id, c.attempts); err != nil {
			t.Fatal(err)
		}
		resumed, err := resumeTransition(ctx, exec, id)
		if err != nil {
			t.Fatalf("%s: %v", c.status, err)
		}
		if resumed != c.want {
			t.Errorf("%s (attempts %d): resumed = %v, want %v", c.status, c.attempts, resumed, c.want)
		}
		var status string
		var queue *string
		var attempts int
		if err := pool.QueryRow(ctx, "SELECT status, queue_name, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid = $1", id).
			Scan(&status, &queue, &attempts); err != nil {
			t.Fatal(err)
		}
		if c.want {
			if status != string(dbos.WorkflowStatusEnqueued) || queue == nil || *queue != internalQueueName || attempts != 0 {
				t.Errorf("%s after resume: status=%s queue=%v attempts=%d, want ENQUEUED on the internal queue with attempts reset", c.status, status, queue, attempts)
			}
		} else if status != c.status || attempts != c.attempts {
			t.Errorf("%s (attempts %d) after refused resume: status=%s attempts=%d, want untouched", c.status, c.attempts, status, attempts)
		}
	}

	// A recorded queue survives; DBOS's own resume would overwrite it.
	seedWorkflow(t, pool, "resume-guard-queued", "CANCELLED", "resume-guard-exec", "resume-guard-v")
	if _, err := pool.Exec(ctx, "UPDATE dbos.workflow_status SET queue_name = 'resume-guard-q' WHERE workflow_uuid = 'resume-guard-queued'"); err != nil {
		t.Fatal(err)
	}
	if resumed, err := resumeTransition(ctx, exec, "resume-guard-queued"); err != nil || !resumed {
		t.Fatalf("queued: resumed=%v err=%v", resumed, err)
	}
	var queue string
	if err := pool.QueryRow(ctx, "SELECT queue_name FROM dbos.workflow_status WHERE workflow_uuid = 'resume-guard-queued'").Scan(&queue); err != nil {
		t.Fatal(err)
	}
	if queue != "resume-guard-q" {
		t.Errorf("queue after resume = %q, want the run's own queue kept", queue)
	}

	if resumed, err := resumeTransition(ctx, exec, "resume-guard-missing"); err != nil || resumed {
		t.Errorf("unknown run: resumed=%v err=%v, want false and no error", resumed, err)
	}
}

// TestResumeRunExplainsRefusal pins what Resume reports when the transition
// did not apply: the classification comes from one status read afterwards,
// and a transition that applied never reads status at all.
func TestResumeRunExplainsRefusal(t *testing.T) {
	fake := func(resumed bool, found []dbos.WorkflowStatus) runStore {
		return runStore{
			resume: func(string) (bool, error) { return resumed, nil },
			list: func(...dbos.ListWorkflowsOption) ([]dbos.WorkflowStatus, error) {
				if resumed {
					t.Fatal("status read after a successful transition")
				}
				return found, nil
			},
		}
	}
	row := func(status dbos.WorkflowStatusType, attempts int) []dbos.WorkflowStatus {
		return []dbos.WorkflowStatus{{ID: "r", Status: status, Attempts: attempts, ExecutorID: "exec-1"}}
	}

	if err := resumeRun(fake(true, nil), "r"); err != nil {
		t.Errorf("applied transition: err = %v, want nil", err)
	}
	if err := resumeRun(fake(false, nil), "r"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("unknown run: err = %v, want ErrRunNotFound", err)
	}
	if err := resumeRun(fake(false, row(dbos.WorkflowStatusPending, 1)), "r"); !errors.Is(err, ErrRunActive) {
		t.Errorf("pending (raced by another Resume or a worker): err = %v, want ErrRunActive", err)
	}
	if err := resumeRun(fake(false, row(dbos.WorkflowStatusSuccess, 1)), "r"); !errors.Is(err, ErrRunTerminal) {
		t.Errorf("success: err = %v, want ErrRunTerminal", err)
	}
	err := resumeRun(fake(false, row(dbos.WorkflowStatusCancelled, 2)), "r")
	if !errors.Is(err, ErrRunInFlight) || !strings.Contains(err.Error(), "exec-1") || !strings.Contains(err.Error(), "ForkFromStage") {
		t.Errorf("cancelled after starting: err = %v, want ErrRunInFlight naming the executor and ForkFromStage", err)
	}
	err = resumeRun(fake(false, row(dbos.WorkflowStatusCancelled, 0)), "r")
	if err == nil || !strings.Contains(err.Error(), "changed concurrently") {
		t.Errorf("cancelled-unstarted again since the transition: err = %v, want the concurrent-change report", err)
	}
	if err := resumeRun(fake(false, nil), ""); err == nil || !strings.Contains(err.Error(), "Resume requires") {
		t.Errorf("empty ID: err = %v, want a usage error", err)
	}
}
