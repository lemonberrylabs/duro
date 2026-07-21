package duro

import (
	"errors"
	"fmt"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Fork describes where to restart an existing pipeline run. WorkflowID and
// Stage are required; every other field is optional and its zero value means
// "inherit from the original run".
type Fork struct {
	WorkflowID string // the pipeline run to fork
	Stage      string // the stage to restart from (its first execution, for stages that ran per item)

	// ForkedID names the forked run; auto-generated when empty.
	ForkedID string
	// ApplicationVersion pins the forked run to a different code version —
	// the recovery tool for rerunning a workflow on fixed code after a bad
	// deploy.
	ApplicationVersion string
	// Queue enqueues the forked run on the named queue instead of running it
	// on the internal one; QueuePartitionKey partitions it there.
	Queue             string
	QueuePartitionKey string
}

// ForkFromStage restarts an existing pipeline run from a named stage: stages
// before it replay from the original run's checkpoints, the named stage and
// everything after re-execute. The pipeline's shape guard replays too, so a
// fork onto changed pipeline code fails fast instead of misreading
// checkpoints.
func ForkFromStage[R any](ctx Context, f Fork) (Handle[R], error) {
	ctx = unwrapContext(ctx)
	if f.WorkflowID == "" {
		return Handle[R]{}, errors.New("duro: ForkFromStage requires a workflow ID")
	}
	if f.Stage == "" {
		return Handle[R]{}, errors.New("duro: ForkFromStage requires a stage name")
	}
	steps, err := dbos.GetWorkflowSteps(ctx, f.WorkflowID, dbos.WithStepsLoadOutput(false))
	if err != nil {
		return Handle[R]{}, fmt.Errorf("duro: ForkFromStage: listing steps of %s: %w", f.WorkflowID, err)
	}
	startStep := -1
	for _, step := range steps {
		if step.StepName == f.Stage && (startStep == -1 || step.StepID < startStep) {
			startStep = step.StepID
		}
	}
	if startStep == -1 {
		return Handle[R]{}, fmt.Errorf("duro: ForkFromStage: workflow %s recorded no execution of stage %q", f.WorkflowID, f.Stage)
	}
	return newHandle(dbos.ForkWorkflow[R](ctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: f.WorkflowID,
		ForkedWorkflowID:   f.ForkedID,
		StartStep:          uint(startStep), //nolint:gosec // step IDs are small non-negative ints
		ApplicationVersion: f.ApplicationVersion,
		QueueName:          f.Queue,
		QueuePartitionKey:  f.QueuePartitionKey,
	}))
}
