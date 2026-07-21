package duro

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/samber/ro"
)

// ShapeStepName is the name of the hidden bookkeeping step Run records as the
// pipeline's first checkpoint. It holds the pipeline's shape fingerprint,
// which Run compares on replay to fail fast on non-deterministic pipeline
// construction.
const ShapeStepName = "duro.shape"

// Run executes the pipeline durably inside a DBOS workflow, feeding it the
// input value and blocking until completion. It returns the last emitted
// value, the first stage error, or ErrNoValue if the pipeline emits nothing.
// Call it as the body of a registered DBOS workflow function.
func Run[P, R any](ctx Context, in P, p Pipeline[P, R]) (R, error) {
	var zero R
	values, err := RunAll(ctx, in, p)
	if err != nil {
		return zero, err
	}
	if len(values) == 0 {
		return zero, ErrNoValue
	}
	return values[len(values)-1], nil
}

// RunAll is Run for pipelines whose final stage legitimately emits multiple
// items: it returns every emitted value.
func RunAll[P, R any](ctx Context, in P, p Pipeline[P, R]) ([]R, error) {
	ctx = unwrapContext(ctx)
	if err := verifyShape(ctx, p.fingerprint()); err != nil {
		return nil, err
	}

	state := &pipelineState{dctx: ctx, gid: goroutineID()}
	subCtx := context.WithValue(ctx, pipelineStateKey{}, state)

	values, _, err := ro.CollectWithContext(subCtx, p.apply(ro.Of(in)))
	return values, err
}

// verifyShape checkpoints the pipeline fingerprint as the workflow's first
// step. On a fresh run this records it; on replay DBOS returns the recorded
// fingerprint, and a mismatch means the workflow constructed a different
// pipeline than the original run — failing here prevents stages from reading
// checkpoints that belong to other stages.
func verifyShape(ctx Context, fingerprint string) error {
	recorded, err := dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		return fingerprint, nil
	}, dbos.WithStepName(ShapeStepName))
	if err != nil {
		return fmt.Errorf("duro: verifying pipeline shape: %w", err)
	}
	if recorded != fingerprint {
		return fmt.Errorf("duro: pipeline shape mismatch on replay: recorded [%s], constructed [%s] — pipeline construction must be deterministic", recorded, fingerprint)
	}
	return nil
}

// goroutineID parses the current goroutine's ID from the runtime stack header
// ("goroutine 123 [running]:"). Costs a few microseconds — negligible next to
// the database write every durable stage performs.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
