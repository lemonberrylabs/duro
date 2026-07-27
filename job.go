package duro

import "fmt"

// Job is a registered pipeline's durable identity: its workflow name and both
// of its types in one declaration, shared by the process that registers the
// pipeline and every process that enqueues or awaits it.
//
// It exists because a workflow name crossing a process boundary is otherwise
// three unchecked assumptions at once. Enqueuing by bare string, a typo
// produces a run no worker can ever dispatch — inserted successfully, reported
// as a healthy "enqueued", and waiting forever; a wrong input type fails to
// deserialize in another process long after the call site; a wrong result type
// surfaces when the caller reads the result. A Job makes all three the
// compiler's problem: declare it once beside the pipeline and both sides refer
// to the same value.
//
//	// package jobs, imported by the worker and the web tier alike
//	var Invoice = duro.NewJob[Batch, Invoice]("invoice")
//
//	// worker
//	duro.RegisterJob(app, jobs.Invoice, invoicePipeline)
//
//	// web tier — name and both types come from the same declaration
//	handle, err := duro.Enqueue(client, Queue, jobs.Invoice, batch)
//
// The zero Job is invalid; build one with NewJob.
type Job[P, R any] struct {
	name string
}

// NewJob declares a pipeline's cross-process identity. Declaring is side-effect
// free — package level is the point — and registration happens separately
// through RegisterJob.
func NewJob[P, R any](name string) Job[P, R] {
	if name == "" {
		panic("duro: NewJob requires a non-empty workflow name")
	}
	return Job[P, R]{name: name}
}

// Name returns the workflow name the job is registered and enqueued under.
func (j Job[P, R]) Name() string { return j.name }

// mustValidJob panics when a job did not come from NewJob, mirroring the
// construction-time checks stages and queues apply. A zero-value Job would
// otherwise register or enqueue under the empty name.
func mustValidJob[P, R any](what string, j Job[P, R]) {
	if j.name == "" {
		panic(fmt.Sprintf("duro: %s requires a job built by NewJob", what))
	}
}
