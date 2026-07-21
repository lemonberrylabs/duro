package duro_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- step option test workflows ---------------------------------------------

var (
	permanentAttempts atomic.Int64
	transientAttempts atomic.Int64
)

// retryPredicateWorkflow exercises the full retry option set: transient
// errors retry (with tight, bounded backoff so the test stays fast) while
// permanent errors stop immediately despite the retry budget.
func retryPredicateWorkflow(ctx dbos.DBOSContext, mode string) (string, error) {
	return duro.Run(ctx, mode, duro.Pipe1(
		duro.Step("classify", func(_ context.Context, m string) (string, error) {
			if m == "permanent" {
				permanentAttempts.Add(1)
				return "", errors.New("permanent: bad input")
			}
			if transientAttempts.Add(1) < 3 {
				return "", errors.New("transient: flaky dependency")
			}
			return "ok:" + m, nil
		},
			duro.WithMaxRetries(5),
			duro.WithBaseInterval(time.Millisecond),
			duro.WithBackoffFactor(1.5),
			duro.WithMaxInterval(5*time.Millisecond),
			duro.WithRetryPredicate(func(err error) bool {
				return !strings.Contains(err.Error(), "permanent")
			}),
		),
	))
}

// stepTimeoutWorkflow hangs in a stage unless the per-attempt timeout fires.
func stepTimeoutWorkflow(ctx dbos.DBOSContext, hang bool) (string, error) {
	return duro.Run(ctx, hang, duro.Pipe1(
		duro.Step("maybe-hang", func(stepCtx context.Context, h bool) (string, error) {
			if !h {
				return "fast", nil
			}
			select {
			case <-stepCtx.Done():
				return "", stepCtx.Err()
			case <-time.After(30 * time.Second):
				return "hung", nil
			}
		}, duro.WithTimeout(100*time.Millisecond)),
	))
}

func registerOptionWorkflows(ctx dbos.DBOSContext) {
	dbos.RegisterWorkflow(ctx, retryPredicateWorkflow, dbos.WithWorkflowName("retryPredicateWorkflow"))
	dbos.RegisterWorkflow(ctx, stepTimeoutWorkflow, dbos.WithWorkflowName("stepTimeoutWorkflow"))
}

// --- tests ------------------------------------------------------------------

// TestRetryPredicateRetriesTransientErrors proves transient errors consume
// retries (with the configured backoff) until the stage succeeds.
func TestRetryPredicateRetriesTransientErrors(t *testing.T) {
	transientAttempts.Store(0)

	result, wfID := mustRun(t, retryPredicateWorkflow, "transient")
	if result != "ok:transient" {
		t.Errorf("result = %q, want %q", result, "ok:transient")
	}
	if got := transientAttempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3 (two transient failures, then success)", got)
	}
	// Retries stay internal to the step: one recorded checkpoint.
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "classify"})
}

// TestRetryPredicateStopsOnPermanentErrors proves the predicate short-circuits
// the retry budget: a non-retryable error fails after exactly one attempt.
func TestRetryPredicateStopsOnPermanentErrors(t *testing.T) {
	permanentAttempts.Store(0)

	handle, err := dbos.RunWorkflow(dctx, retryPredicateWorkflow, "permanent")
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), "permanent") {
		t.Fatalf("workflow error = %v, want the permanent failure", err)
	}
	if got := permanentAttempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (predicate must stop retries)", got)
	}
}

// TestStepTimeout proves WithTimeout cancels a hung attempt via the step
// context and fails the pipeline promptly.
func TestStepTimeout(t *testing.T) {
	if result, _ := mustRun(t, stepTimeoutWorkflow, false); result != "fast" {
		t.Errorf("result = %q, want %q (timeout must not affect fast stages)", result, "fast")
	}

	start := time.Now()
	handle, err := dbos.RunWorkflow(dctx, stepTimeoutWorkflow, true)
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("workflow error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("hung stage took %v to fail, want prompt cancellation", elapsed)
	}
}
