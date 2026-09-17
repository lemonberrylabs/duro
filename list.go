package duro

import (
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// ListOption narrows, pages, or enriches a ListRuns query. Every filter is
// conjunctive: a run is returned only if it matches all of them.
type ListOption func(*listConfig)

// listConfig accumulates ListOptions.
type listConfig struct {
	filters   []dbos.ListWorkflowsOption
	loadInput bool
	// empty is set when a list filter was given no values: nothing can match
	// "one of these", so ListRuns returns nothing without querying. Treating
	// an empty list as "no filter" instead would turn a caller's empty
	// selection into the whole table — the wrong way to be surprised.
	empty bool
	err   error // the first invalid option; ListRuns reports it instead of querying
}

// filter records a membership filter over n values.
func (c *listConfig) filter(n int, opt dbos.ListWorkflowsOption) {
	if n == 0 {
		c.empty = true
		return
	}
	c.filters = append(c.filters, opt)
}

func (c *listConfig) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

// WithNames restricts the listing to runs of the named pipelines — their
// registered workflow names, as in Register, RegisterJob, or Job.Name. Given
// no names, nothing matches.
func WithNames(names ...string) ListOption {
	return func(c *listConfig) { c.filter(len(names), dbos.WithFilterName(names...)) }
}

// WithStates restricts the listing to runs in any of the given states. Given
// no states, nothing matches.
func WithStates(states ...State) ListOption {
	return func(c *listConfig) {
		statuses := make([]dbos.WorkflowStatusType, len(states))
		for i, s := range states {
			statuses[i] = dbosStatusOf(s)
		}
		c.filter(len(states), dbos.WithFilterStatus(statuses...))
	}
}

// WithIDs restricts the listing to the given workflow IDs. Given no IDs,
// nothing matches. Unlike StatusAll, results follow the listing's sort order,
// not the order of the IDs.
func WithIDs(ids ...string) ListOption {
	return func(c *listConfig) { c.filter(len(ids), dbos.WithFilterWorkflowIDs(ids...)) }
}

// WithApplicationNames restricts an administrative Client listing to the
// named DBOS applications. Engine listings are already scoped to their own
// application (plus unclaimed migrated rows) unless this filter is explicit.
// Unclaimed runs — recorded with no application name: every run written before
// DBOS v1, and a nameless Client's enqueues until a worker claims them — match
// whatever names are given. DBOS's filter cannot exclude them; tell them apart
// by an empty RunStatus.ApplicationName.
func WithApplicationNames(names ...string) ListOption {
	return func(c *listConfig) { c.filter(len(names), dbos.WithFilterApplicationName(names...)) }
}

// WithScheduleNames restricts the listing to runs created by any of the named
// database-backed schedules. Given no names, nothing matches. Scheduled Duro
// pipelines run through an internal adapter, so use this option—not WithNames—
// to select their public RegisterScheduled names.
func WithScheduleNames(names ...string) ListOption {
	return func(c *listConfig) { c.filter(len(names), dbos.WithFilterScheduleName(names...)) }
}

// WithCreatedAfter restricts the listing to runs created at or after t. A
// zero time applies no bound.
func WithCreatedAfter(t time.Time) ListOption {
	return func(c *listConfig) { c.filters = append(c.filters, dbos.WithFilterCreatedAfter(t)) }
}

// WithCreatedBefore restricts the listing to runs created at or before t. A
// zero time applies no bound.
func WithCreatedBefore(t time.Time) ListOption {
	return func(c *listConfig) { c.filters = append(c.filters, dbos.WithFilterCreatedBefore(t)) }
}

// WithQueue restricts the listing to runs currently recorded on the named
// queue (a Queue's Name). DBOS clears a run's queue when it is cancelled or
// exceeds its recovery attempts, so those runs match no queue.
func WithQueue(name string) ListOption {
	return func(c *listConfig) {
		if name == "" {
			c.fail(errors.New("duro: ListRuns: WithQueue requires a queue name"))
			return
		}
		c.filters = append(c.filters, dbos.WithFilterQueueName(name))
	}
}

// WithLimit caps the number of runs returned; page with WithOffset. Without
// it the listing is unbounded. n must be positive: ListRuns reports a zero or
// negative limit as an error rather than silently returning nothing.
func WithLimit(n int) ListOption {
	return func(c *listConfig) {
		if n <= 0 {
			c.fail(fmt.Errorf("duro: ListRuns: WithLimit(%d): limit must be positive", n))
			return
		}
		c.filters = append(c.filters, dbos.WithFilterLimit(n))
	}
}

// WithOffset skips the first n runs of the listing, for paging with
// WithLimit. n must not be negative.
func WithOffset(n int) ListOption {
	return func(c *listConfig) {
		if n < 0 {
			c.fail(fmt.Errorf("duro: ListRuns: WithOffset(%d): offset must not be negative", n))
			return
		}
		c.filters = append(c.filters, dbos.WithFilterOffset(n))
	}
}

// WithNewestFirst orders the listing by creation time descending. The
// default is oldest first.
func WithNewestFirst() ListOption {
	return func(c *listConfig) { c.filters = append(c.filters, dbos.WithFilterSortDesc()) }
}

// WithInput loads each run's stored input into RunStatus.Input, as JSON
// text. It is off by default so the listing stays payload-free like StatusAll.
func WithInput() ListOption {
	return func(c *listConfig) { c.loadInput = true }
}

// ListRuns lists durable runs filtered, paged, and ordered by the options. An
// App sees its own application's runs plus migrated unclaimed rows by default;
// a nameless Client sees every application, and WithApplicationNames selects
// explicit owners. With no options the matching runs are returned oldest
// first; page with WithLimit and WithOffset. Runs are reported through the same
// mapping as Status, with the same failed-run treatment: a failed run's
// recorded error is fetched in a second query scoped to the failed runs on the
// page, so RunStatus.Err is populated exactly as Status would populate it. No
// payloads are loaded unless WithInput asks for the input.
//
//	runs, err := duro.ListRuns(app,
//		duro.WithNames("invoice"),
//		duro.WithStates(duro.StateError, duro.StateRetriesExceeded),
//		duro.WithNewestFirst(), duro.WithLimit(50))
//
// Client.ListRuns is the same query from an enqueue-only process.
func ListRuns(ctx Context, opts ...ListOption) ([]RunStatus, error) {
	return listRunsWith(engineStore(unwrapContext(ctx), nil), opts)
}

// listRunsWith is the core behind ListRuns and Client.ListRuns.
func listRunsWith(store runStore, opts []ListOption) ([]RunStatus, error) {
	var cfg listConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.err != nil {
		return nil, cfg.err
	}
	if cfg.empty {
		return []RunStatus{}, nil
	}
	return listRuns(store, cfg.loadInput, cfg.filters...)
}

// StepStatus is the record of one step a run has executed: its position,
// name, outcome, and timing. Step outputs are never decoded or returned.
type StepStatus struct {
	ID   int    // position in the run's step sequence, from 0
	Name string // the stage name; ShapeStepName for the shape checkpoint that opens every pipeline run
	// Err is the error the step recorded, nil when it succeeded. A recorded
	// step error is final for that step: retries happen before recording.
	Err error
	// ChildID is the child run this step started — set for FanOut children
	// and other child workflows, "" for ordinary steps.
	ChildID     string
	StartedAt   time.Time
	CompletedAt time.Time
}

// Steps lists the steps a run has executed so far, in execution order —
// what a pipeline has checkpointed, which is exactly what replays on
// recovery. A pipeline run opens with the ShapeStepName checkpoint, then one
// entry per durable stage execution (per item for stages that ran per item;
// Pure stages record nothing). A run that has not started yet has no steps.
// Unknown IDs return ErrRunNotFound.
//
// Step outputs are never decoded or returned, but DBOS's step query still
// reads the output column, so inspecting a run whose stages checkpoint large
// values costs that bandwidth — Steps is cheap in memory, not free on the
// wire. Prefer Status for polling; reserve Steps for inspection.
//
// Client.Steps is the same query from an enqueue-only process.
func Steps(ctx Context, workflowID string) ([]StepStatus, error) {
	return runSteps(engineStore(unwrapContext(ctx), nil), workflowID)
}

// runSteps is the core behind Steps and Client.Steps.
func runSteps(store runStore, workflowID string) ([]StepStatus, error) {
	if workflowID == "" {
		return nil, errors.New("duro: Steps requires a workflow ID")
	}
	steps, err := store.steps(workflowID)
	if err != nil {
		return nil, fmt.Errorf("duro: fetching steps of %s: %w", workflowID, err)
	}
	if len(steps) == 0 {
		// No steps is what both an unknown run and a run that has not started
		// look like; one extra payload-free query tells them apart.
		found, err := store.list(
			dbos.WithFilterWorkflowIDs(workflowID),
			dbos.WithFilterLoadInput(false),
			dbos.WithFilterLoadOutput(false),
		)
		if err != nil {
			return nil, fmt.Errorf("duro: fetching run status: %w", err)
		}
		if len(found) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrRunNotFound, workflowID)
		}
		return []StepStatus{}, nil
	}
	out := make([]StepStatus, len(steps))
	for i, s := range steps {
		out[i] = StepStatus{
			ID:          s.StepID,
			Name:        s.StepName,
			Err:         s.Error,
			ChildID:     s.ChildWorkflowID,
			StartedAt:   s.StartedAt,
			CompletedAt: s.CompletedAt,
		}
	}
	return out, nil
}
