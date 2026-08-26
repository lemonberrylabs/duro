package duro_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// RunStatus and StepStatus stay comparable: callers compare them and key maps
// on them, and a slice-typed field would break that at compile time.
var (
	_ = map[duro.RunStatus]bool{}
	_ = map[duro.StepStatus]bool{}
)

// startAndAwait starts a registeredWf run and waits for it, spacing starts a
// few milliseconds apart so creation timestamps (millisecond resolution)
// order deterministically.
func startAndAwait(t *testing.T, n int) string {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	h, err := registeredWf.Start(app, n)
	if err != nil {
		t.Fatalf("starting: %v", err)
	}
	if _, err := h.Result(); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return h.ID()
}

// TestListRunsByName proves ListRuns finds runs by registered name, orders
// and pages them, and fills the cheap per-run columns without loading
// payloads.
func TestListRunsByName(t *testing.T) {
	first := startAndAwait(t, 1)
	second := startAndAwait(t, 2)
	time.Sleep(5 * time.Millisecond)
	other, err := dbos.RunWorkflow(dctx, plainLinearWorkflow, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.GetResult(); err != nil {
		t.Fatal(err)
	}

	runs, err := duro.ListRuns(app,
		duro.WithNames("registeredPipeline"),
		duro.WithIDs(first, second, other.GetWorkflowID()), // filters are conjunctive: the other pipeline's run drops out
		duro.WithNewestFirst())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 || runs[0].ID != second || runs[1].ID != first {
		t.Fatalf("runs = %+v, want [%s %s] newest first", runs, second, first)
	}
	for _, r := range runs {
		if r.Name != "registeredPipeline" || r.State != duro.StateSuccess || r.Err != nil {
			t.Errorf("run %s = name %q state %s err %v, want a successful registeredPipeline run", r.ID, r.Name, r.State, r.Err)
		}
		if !r.StartedAt.IsZero() || r.CompletedAt.IsZero() {
			t.Errorf("run %s timestamps: started %v completed %v; want completed set and started zero (DBOS stamps a start only on dequeue)", r.ID, r.StartedAt, r.CompletedAt)
		}
		if r.Attempts != 1 || r.ExecutorID == "" || r.ApplicationVersion != appVersion.Load() {
			t.Errorf("run %s attempts/executor/version = %d/%q/%q, want 1/<set>/%q", r.ID, r.Attempts, r.ExecutorID, r.ApplicationVersion, appVersion.Load())
		}
		if r.QueueName != "" || r.ParentID != "" || r.Input != "" {
			t.Errorf("run %s queue/parent/input = %q/%q/%s, want all empty for a direct top-level run listed without WithInput", r.ID, r.QueueName, r.ParentID, r.Input)
		}
	}

	page, err := duro.ListRuns(app, duro.WithIDs(first, second), duro.WithLimit(1), duro.WithOffset(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || page[0].ID != second {
		t.Errorf("page 2 of size 1 (oldest first) = %+v, want [%s]", page, second)
	}
}

// TestListRunsFilters proves the state, time-window, and empty-filter
// semantics, and that a failed run's recorded error is populated exactly as
// Status populates it.
func TestListRunsFilters(t *testing.T) {
	ok := startAndAwait(t, 4)
	time.Sleep(5 * time.Millisecond)
	failed, err := dbos.RunWorkflow(dctx, retryPredicateWorkflow, "permanent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failed.GetResult(); err == nil {
		t.Fatal("expected the run to fail")
	}
	failedID := failed.GetWorkflowID()
	ids := duro.WithIDs(ok, failedID)

	errored, err := duro.ListRuns(app, ids, duro.WithStates(duro.StateError, duro.StateRetriesExceeded))
	if err != nil {
		t.Fatal(err)
	}
	if len(errored) != 1 || errored[0].ID != failedID {
		t.Fatalf("WithStates(error, retries_exceeded) = %+v, want [%s]", errored, failedID)
	}
	if errored[0].Err == nil || !strings.Contains(errored[0].Err.Error(), "permanent") {
		t.Errorf("failed run Err = %v, want the recorded permanent failure", errored[0].Err)
	}
	succeeded, err := duro.ListRuns(app, ids, duro.WithStates(duro.StateSuccess))
	if err != nil {
		t.Fatal(err)
	}
	if len(succeeded) != 1 || succeeded[0].ID != ok {
		t.Errorf("WithStates(success) = %+v, want [%s]", succeeded, ok)
	}

	okStatus, err := duro.Status(app, ok)
	if err != nil {
		t.Fatal(err)
	}
	window, err := duro.ListRuns(app, ids, duro.WithCreatedAfter(okStatus.CreatedAt), duro.WithCreatedBefore(okStatus.CreatedAt))
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 1 || window[0].ID != ok {
		t.Errorf("created-at window around the first run = %+v, want just [%s]", window, ok)
	}
	later, err := duro.ListRuns(app, ids, duro.WithCreatedAfter(okStatus.CreatedAt.Add(time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	if len(later) != 1 || later[0].ID != failedID {
		t.Errorf("created after the first run = %+v, want just [%s]", later, failedID)
	}

	// An empty membership filter matches nothing — it never widens to "all".
	for name, opt := range map[string]duro.ListOption{
		"WithNames()":  duro.WithNames(),
		"WithStates()": duro.WithStates(),
		"WithIDs()":    duro.WithIDs(),
	} {
		none, err := duro.ListRuns(app, ids, opt)
		if err != nil || len(none) != 0 {
			t.Errorf("%s = %+v, %v; want no runs and no error", name, none, err)
		}
	}
}

// TestListRunsRejectsInvalidOptions proves misuse fails at the call instead of
// silently returning nothing or everything.
func TestListRunsRejectsInvalidOptions(t *testing.T) {
	for name, opt := range map[string]duro.ListOption{
		"WithLimit(0)":    duro.WithLimit(0),
		"WithLimit(-1)":   duro.WithLimit(-1),
		"WithOffset(-1)":  duro.WithOffset(-1),
		"WithQueue(\"\")": duro.WithQueue(""),
	} {
		if _, err := duro.ListRuns(app, opt); err == nil || !strings.Contains(err.Error(), "ListRuns") {
			t.Errorf("%s error = %v, want a ListRuns usage error", name, err)
		}
	}
}

// TestListRunsWithInput proves WithInput exposes the stored input as JSON —
// and that nothing else loads it.
func TestListRunsWithInput(t *testing.T) {
	scalar := startAndAwait(t, 7)
	batch, err := handWrittenWf.Start(app, []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Result(); err != nil {
		t.Fatal(err)
	}

	runs, err := duro.ListRuns(app, duro.WithIDs(scalar, batch.ID()), duro.WithInput())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want 2", len(runs))
	}
	want := map[string]string{scalar: "7", batch.ID(): "[1,2,3]"}
	for _, r := range runs {
		if r.Input != want[r.ID] {
			t.Errorf("run %s Input = %s, want %s", r.ID, r.Input, want[r.ID])
		}
	}
	bare, err := duro.ListRuns(app, duro.WithIDs(scalar))
	if err != nil {
		t.Fatal(err)
	}
	if len(bare) != 1 || bare[0].Input != "" {
		t.Errorf("without WithInput, Input = %q, want empty", bare[0].Input)
	}
	if s, _ := duro.Status(app, scalar); s.Input != "" {
		t.Errorf("Status Input = %q, want empty: the status path never loads payloads", s.Input)
	}
}

// TestListRunsFanOutChildren proves the parent/child and queue columns: each
// FanOut child reports its parent run and its queue, the parent's steps
// carry the child IDs, and WithQueue selects the children.
func TestListRunsFanOutChildren(t *testing.T) {
	resetFanCounters()
	h, err := dbos.RunWorkflow(dctx, fanOutWorkflow, []int{2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if sum, err := h.GetResult(); err != nil || sum != 29 {
		t.Fatalf("fan-out result = %d, %v; want 29", sum, err)
	}
	parentID := h.GetWorkflowID()
	parent, err := duro.Status(app, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if parent.ParentID != "" {
		t.Errorf("top-level run ParentID = %q, want empty", parent.ParentID)
	}

	listed, err := duro.ListRuns(app,
		duro.WithNames("fanChildSquare"),
		duro.WithQueue(fanQueueName),
		duro.WithCreatedAfter(parent.CreatedAt))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	children := make(map[string]bool)
	for _, c := range listed {
		if c.ParentID != parentID {
			continue // another test's fan-out, created concurrently
		}
		children[c.ID] = true
		if c.QueueName != fanQueueName || c.State != duro.StateSuccess {
			t.Errorf("child %s queue/state = %q/%s, want %s/success", c.ID, c.QueueName, c.State, fanQueueName)
		}
	}
	if len(children) != 3 {
		t.Fatalf("found %d children of %s, want 3 (listed: %+v)", len(children), parentID, listed)
	}

	steps, err := duro.Steps(app, parentID)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	spawned := make(map[string]bool)
	for _, s := range steps {
		if s.ChildID != "" {
			spawned[s.ChildID] = true
		}
	}
	if len(spawned) != 3 {
		t.Fatalf("parent steps name %d children, want 3: %+v", len(spawned), steps)
	}
	for id := range children {
		if !spawned[id] {
			t.Errorf("child %s is not named by any parent step", id)
		}
	}
}

// TestSteps proves Steps reports a run's checkpoints in order — the shape
// checkpoint first, then each durable stage — with outcomes and timings, and
// resolves unknown and not-yet-started runs distinctly.
func TestSteps(t *testing.T) {
	id := startAndAwait(t, 8)
	steps, err := duro.Steps(app, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	names := make([]string, len(steps))
	for i, s := range steps {
		names[i] = s.Name
		if s.ID != i {
			t.Errorf("step %d has ID %d, want sequential from 0", i, s.ID)
		}
		if s.Err != nil || s.ChildID != "" || s.StartedAt.IsZero() || s.CompletedAt.IsZero() {
			t.Errorf("step %+v: want no error, no child, and both timestamps set", s)
		}
	}
	if want := []string{duro.ShapeStepName, "double", "plus-one"}; strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("step names = %v, want %v", names, want)
	}

	failed, err := dbos.RunWorkflow(dctx, retryPredicateWorkflow, "permanent")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = failed.GetResult()
	failedSteps, err := duro.Steps(app, failed.GetWorkflowID())
	if err != nil {
		t.Fatal(err)
	}
	if len(failedSteps) != 2 || failedSteps[1].Name != "classify" || failedSteps[1].Err == nil ||
		!strings.Contains(failedSteps[1].Err.Error(), "permanent") {
		t.Errorf("failed run steps = %+v, want [shape classify(err permanent)]", failedSteps)
	}

	if _, err := duro.Steps(app, "list-no-such-run"); !errors.Is(err, duro.ErrRunNotFound) {
		t.Errorf("Steps(unknown) = %v, want ErrRunNotFound", err)
	}
	if _, err := duro.Steps(app, ""); err == nil || !strings.Contains(err.Error(), "Steps requires") {
		t.Errorf("Steps(\"\") = %v, want a usage error", err)
	}

	const waiting = "list-steps-not-started"
	startPinnedToNobody(t, waiting)
	none, err := duro.Steps(app, waiting)
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("Steps(enqueued, never started) = %+v, %v; want an empty list and no error", none, err)
	}
	if err := duro.Cancel(app, waiting); err != nil {
		t.Fatal(err)
	}
}

// TestListRunsIncludesCancelWatcher proves duro's own durable plumbing is
// listable under its exported names: a cancel-enabled FanOut leaves a watcher
// run named CancelWatcherName on the CancelWatchQueueName queue, parented to
// the batch's run — visible to an operator, and filterable by name.
func TestListRunsIncludesCancelWatcher(t *testing.T) {
	if duro.CancelWatcherName != "duro.cancel-watcher" || duro.CancelWatchQueueName != "duro.cancel-watch" {
		t.Fatalf("watcher identities = %q/%q: these are durable names and must never change", duro.CancelWatcherName, duro.CancelWatchQueueName)
	}
	h, err := dbos.RunWorkflow(dctx, fanCancelShortWorkflow, []int{7001, 7002})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.GetResult(); err != nil {
		t.Fatalf("fan-out failed: %v", err)
	}
	parent, err := duro.Status(app, h.GetWorkflowID())
	if err != nil {
		t.Fatal(err)
	}

	watchers, err := duro.ListRuns(app,
		duro.WithNames(duro.CancelWatcherName),
		duro.WithQueue(duro.CancelWatchQueueName),
		duro.WithCreatedAfter(parent.CreatedAt))
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	var mine []duro.RunStatus
	for _, w := range watchers {
		if w.ParentID == parent.ID {
			mine = append(mine, w)
		}
	}
	if len(mine) != 1 || mine[0].Name != duro.CancelWatcherName || mine[0].QueueName != duro.CancelWatchQueueName {
		t.Errorf("watchers of %s = %+v, want exactly one named %s on %s", parent.ID, mine, duro.CancelWatcherName, duro.CancelWatchQueueName)
	}
	steps, err := duro.Steps(app, parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	spawnedWatcher := false
	for _, s := range steps {
		if len(mine) == 1 && s.ChildID == mine[0].ID {
			spawnedWatcher = true
		}
	}
	if !spawnedWatcher {
		t.Errorf("no step of %s records the watcher as its child: %+v", parent.ID, steps)
	}
}
