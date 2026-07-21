package duro

import (
	"errors"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Handle tracks one durable pipeline run. It is returned by Start and
// ForkFromStage; Result blocks until the run completes.
type Handle[R any] struct {
	h dbos.WorkflowHandle[R]
}

func newHandle[R any](h dbos.WorkflowHandle[R], err error) (Handle[R], error) {
	if err != nil {
		return Handle[R]{}, err
	}
	return Handle[R]{h: h}, nil
}

// ID returns the run's workflow ID.
func (h Handle[R]) ID() string {
	if h.h == nil {
		return ""
	}
	return h.h.GetWorkflowID()
}

// Result blocks until the run completes and returns its result or error.
func (h Handle[R]) Result() (R, error) {
	var zero R
	if h.h == nil {
		return zero, errors.New("duro: empty handle")
	}
	return h.h.GetResult()
}

// Status returns the run's current status record.
func (h Handle[R]) Status() (dbos.WorkflowStatus, error) {
	if h.h == nil {
		return dbos.WorkflowStatus{}, errors.New("duro: empty handle")
	}
	return h.h.GetStatus()
}
