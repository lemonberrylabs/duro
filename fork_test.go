package duro_test

import (
	"strings"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// TestForkFromStage proves forking by stage name: stages before the named one
// replay from checkpoints (no re-execution), the named stage and everything
// after re-run.
func TestForkFromStage(t *testing.T) {
	doubleCount.Store(0)
	addThreeCount.Store(0)
	stringifyCount.Store(0)

	original, wfID := mustRun(t, linearWorkflow, 5)
	if original != "value=13" {
		t.Fatalf("original result = %q, want %q", original, "value=13")
	}

	handle, err := duro.ForkFromStage[string](dctx, duro.Fork{WorkflowID: wfID, Stage: "add-three"})
	if err != nil {
		t.Fatalf("forking from stage: %v", err)
	}
	forked, err := handle.Result()
	if err != nil {
		t.Fatalf("forked workflow failed: %v", err)
	}
	if forked != original {
		t.Errorf("forked result = %q, want %q", forked, original)
	}

	// "double" replays; "add-three" and "stringify" re-execute.
	if d, a, s := doubleCount.Load(), addThreeCount.Load(), stringifyCount.Load(); d != 1 || a != 2 || s != 2 {
		t.Errorf("double/add-three/stringify executions = %d/%d/%d, want 1/2/2", d, a, s)
	}
}

// TestForkFromStageWithApplicationVersion proves the version override reaches
// the forked run — the recovery path for rerunning on pinned code.
func TestForkFromStageWithApplicationVersion(t *testing.T) {
	_, wfID := mustRun(t, linearWorkflow, 8)

	origHandle, err := dbos.RetrieveWorkflow[string](dctx, wfID)
	if err != nil {
		t.Fatalf("retrieving original: %v", err)
	}
	origStatus, err := origHandle.GetStatus()
	if err != nil {
		t.Fatalf("original status: %v", err)
	}

	handle, err := duro.ForkFromStage[string](dctx, duro.Fork{
		WorkflowID:         wfID,
		Stage:              "stringify",
		ApplicationVersion: origStatus.ApplicationVersion, // explicit but runnable
	})
	if err != nil {
		t.Fatalf("forking with version: %v", err)
	}
	if _, err := handle.Result(); err != nil {
		t.Fatalf("forked workflow failed: %v", err)
	}

	status, err := handle.Status()
	if err != nil {
		t.Fatalf("forked status: %v", err)
	}
	if status.ApplicationVersion != origStatus.ApplicationVersion {
		t.Errorf("forked application version = %q, want %q", status.ApplicationVersion, origStatus.ApplicationVersion)
	}
	if status.ForkedFrom != wfID {
		t.Errorf("forked-from = %q, want %q", status.ForkedFrom, wfID)
	}
}

// TestForkFromStageOnRegisteredPipeline proves forking works for pipelines
// registered through duro.Register: the forked run re-dispatches to the
// correct configured instance via the recorded (name, config name) pair.
func TestForkFromStageOnRegisteredPipeline(t *testing.T) {
	handle, err := registeredWf.Start(app, 4)
	if err != nil {
		t.Fatalf("starting registered pipeline: %v", err)
	}
	original, err := handle.Result()
	if err != nil {
		t.Fatalf("registered pipeline failed: %v", err)
	}
	if original != 9 {
		t.Fatalf("result = %d, want 9", original)
	}

	forkHandle, err := duro.ForkFromStage[int](app, duro.Fork{
		WorkflowID: handle.ID(),
		Stage:      "plus-one",
	})
	if err != nil {
		t.Fatalf("forking registered pipeline: %v", err)
	}
	forked, err := forkHandle.Result()
	if err != nil {
		t.Fatalf("forked registered pipeline failed: %v", err)
	}
	if forked != original {
		t.Errorf("forked result = %d, want %d", forked, original)
	}
}

// TestForkFromStageUnknownStage proves a helpful error for stage names the
// run never executed.
func TestForkFromStageUnknownStage(t *testing.T) {
	_, wfID := mustRun(t, linearWorkflow, 2)

	_, err := duro.ForkFromStage[string](dctx, duro.Fork{WorkflowID: wfID, Stage: "no-such-stage"})
	if err == nil || !strings.Contains(err.Error(), `no execution of stage "no-such-stage"`) {
		t.Errorf("error = %v, want the unknown-stage error", err)
	}
}
