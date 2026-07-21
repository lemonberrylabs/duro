package duro

import (
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// PipelineWorkflow is a pipeline registered as a DBOS workflow. Every
// registration shares one generic runner method, so DBOS's configured
// instance mechanism keys each registration by the pipeline's name — that is
// what ConfigName returns, and why runs must go through Start (which selects
// this instance) rather than a bare dbos.RunWorkflow.
type PipelineWorkflow[P, R any] struct {
	name string
	p    Pipeline[P, R]
	// wf is the exact function value registered with DBOS. A Go method
	// value's runtime name depends on where it was created, so this single
	// value — created once in Register — is the only one whose name matches
	// the registration; Start, Workflow, and RegisterDebounced all reuse it.
	wf dbos.Workflow[P, R]
}

// ConfigName implements dbos.ConfiguredInstance: the workflow name uniquely
// keys this pipeline's registration.
func (w *PipelineWorkflow[P, R]) ConfigName() string { return w.name }

// run executes the pipeline durably; its method value is what Register
// registers with DBOS.
func (w *PipelineWorkflow[P, R]) run(ctx Context, in P) (R, error) {
	return Run(ctx, in, w.p)
}

// dbosWorkflow and runOptions implement WorkflowRef, so a registered
// pipeline is passable directly as a FanOut child — the ref carries the
// instance selection with it.
func (w *PipelineWorkflow[P, R]) dbosWorkflow() dbos.Workflow[P, R] { return w.wf }
func (w *PipelineWorkflow[P, R]) runOptions() []dbos.WorkflowOption {
	return []dbos.WorkflowOption{dbos.WithRunInstance(w)}
}

// WithWorkflowID assigns a run's workflow ID — the standard idempotency key:
// starting the same ID twice re-attaches to the first run instead of running
// again. Every other dbos.WorkflowOption passes through Start unchanged.
func WithWorkflowID(id string) dbos.WorkflowOption { return dbos.WithWorkflowID(id) }

// RegisteredWorkflow is a hand-written workflow function registered under a
// durable name; see RegisterWorkflow. It is a WorkflowRef, so it passes
// directly as a FanOut child.
type RegisteredWorkflow[P, R any] struct {
	name string
	wf   WorkflowFunc[P, R]
}

// dbosWorkflow and runOptions implement WorkflowRef.
func (w *RegisteredWorkflow[P, R]) dbosWorkflow() WorkflowFunc[P, R]  { return w.wf }
func (w *RegisteredWorkflow[P, R]) runOptions() []dbos.WorkflowOption { return nil }

// Start runs (or, with dbos.WithQueue, enqueues) the workflow and returns
// its handle. It accepts any dbos.WorkflowOption, like PipelineWorkflow's
// Start.
func (w *RegisteredWorkflow[P, R]) Start(ctx Context, in P, opts ...dbos.WorkflowOption) (Handle[R], error) {
	return newHandle(dbos.RunWorkflow(unwrapContext(ctx), w.wf, in, opts...))
}

// RegisterWorkflow registers a hand-written workflow function under the given
// name. Reach for it only when a workflow needs imperative control flow
// around its pipelines — branching between them, looping, post-processing a
// RunAll — since Register covers pipelines themselves. Call it before
// Launch; the name is the workflow's durable identity, so register the same
// function under the same name on every process start. Workflow-level
// registration options pass through opts.
func RegisterWorkflow[P, R any](ctx Context, name string, fn WorkflowFunc[P, R], opts ...dbos.WorkflowRegistrationOption) *RegisteredWorkflow[P, R] {
	if name == "" {
		panic("duro: RegisterWorkflow requires a non-empty workflow name")
	}
	if fn == nil {
		panic(fmt.Sprintf("duro: RegisterWorkflow %q requires a non-nil workflow function", name))
	}
	dbos.RegisterWorkflow(unwrapContext(ctx), fn,
		append([]dbos.WorkflowRegistrationOption{dbos.WithWorkflowName(name)}, opts...)...)
	return &RegisteredWorkflow[P, R]{name: name, wf: fn}
}

// Start runs (or, with dbos.WithQueue, enqueues) the pipeline as a durable
// workflow and returns its handle. It accepts any dbos.WorkflowOption —
// workflow ID, queue, priority, deduplication, auth. For a durable deadline,
// pass a context derived with dbos.WithTimeout.
func (w *PipelineWorkflow[P, R]) Start(ctx Context, in P, opts ...dbos.WorkflowOption) (Handle[R], error) {
	return newHandle(dbos.RunWorkflow(unwrapContext(ctx), w.wf, in, append(opts, dbos.WithRunInstance(w))...))
}

// Register turns a pipeline into a registered DBOS workflow under the given
// name, and registers every queue the pipeline references. Call it after New
// and before Launch; run the result with Start. Workflow-level registration
// options (recovery attempts via dbos.WithMaxRetries, ...) pass through opts.
//
// The name is the pipeline's durable identity: in-flight runs are recovered
// by looking it up, so it must be registered on every process start. Launch
// warns about runs whose name is no longer registered.
func Register[P, R any](ctx Context, name string, p Pipeline[P, R], opts ...dbos.WorkflowRegistrationOption) *PipelineWorkflow[P, R] {
	mustValidPipelineWorkflow("Register", name, p)
	ctx = unwrapContext(ctx)
	for _, q := range p.queues {
		if err := ensureQueue(ctx, q); err != nil {
			panic(fmt.Sprintf("duro: Register %q: %v", name, err))
		}
	}
	w := &PipelineWorkflow[P, R]{name: name, p: p}
	w.wf = w.run
	dbos.RegisterWorkflow(ctx, w.wf,
		append([]dbos.WorkflowRegistrationOption{dbos.WithWorkflowName(name), dbos.WithInstance(w)}, opts...)...)
	return w
}

// RegisterScheduled registers the pipeline as a scheduled (cron) workflow:
// every tick starts a durable run whose input is the scheduled time. The
// schedule uses cron syntax with seconds precision ("*/30 * * * * *" = every
// 30 seconds). Requiring Pipeline[time.Time, R] makes the DBOS rule that
// scheduled workflows take a time.Time input a compile-time guarantee.
func RegisterScheduled[R any](ctx Context, name, cronSchedule string, p Pipeline[time.Time, R], opts ...dbos.WorkflowRegistrationOption) *PipelineWorkflow[time.Time, R] {
	if cronSchedule == "" {
		panic(fmt.Sprintf("duro: RegisterScheduled %q requires a cron schedule", name))
	}
	return Register(ctx, name, p, append([]dbos.WorkflowRegistrationOption{dbos.WithSchedule(cronSchedule)}, opts...)...)
}

// Debouncer collapses bursts of pipeline starts into a single run; see
// RegisterDebounced.
type Debouncer[P, R any] struct {
	d *dbos.Debouncer[P, R]
}

// Debounce postpones the pipeline's start by delay. Every further call with
// the same key pushes the start back and replaces the input; when the delay
// lapses, the pipeline runs once with the last input. Different keys debounce
// independently. Every call returns a handle to the same eventual run.
func (d *Debouncer[P, R]) Debounce(ctx Context, key string, delay time.Duration, input P) (Handle[R], error) {
	return newHandle(d.d.Debounce(unwrapContext(ctx), key, delay, input))
}

// RegisterDebounced registers the pipeline as a workflow and returns its
// debouncer. Cap the total postponement with dbos.WithDebouncerTimeout. Like
// Register, call it after New and before Launch.
func RegisterDebounced[P, R any](ctx Context, name string, p Pipeline[P, R], opts ...dbos.DebouncerOption) *Debouncer[P, R] {
	ctx = unwrapContext(ctx)
	w := Register(ctx, name, p)
	return &Debouncer[P, R]{d: dbos.NewDebouncer(ctx, w.wf, append([]dbos.DebouncerOption{dbos.WithDebouncerInstance(w)}, opts...)...)}
}

// mustValidPipelineWorkflow panics when a pipeline-workflow registration is
// structurally invalid — mirroring the construction-time checks stages apply.
func mustValidPipelineWorkflow[P, R any](what, name string, p Pipeline[P, R]) {
	if name == "" {
		panic(fmt.Sprintf("duro: %s requires a non-empty workflow name", what))
	}
	if p.apply == nil {
		panic(fmt.Sprintf("duro: %s %q requires a pipeline built by Pipe1..Pipe8", what, name))
	}
}
