package duro

import (
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// TestStrandedRuns exercises the launch-time stranded-run detection against
// the same (name, config name) resolution DBOS recovery uses.
func TestStrandedRuns(t *testing.T) {
	registered := []dbos.WorkflowRegistryEntry{
		{Name: "invoice", FQN: "example.com/app.Register[Batch,Invoice]/invoice", ConfigName: "invoice"},
		{FQN: "example.com/app.plainWorkflow"}, // no custom name: recovery looks up the FQN
	}
	active := []dbos.WorkflowStatus{
		{ID: "wf-1", Name: "invoice", ConfigName: new(string("invoice"))},                   // registered instance ✓
		{ID: "wf-2", Name: "example.com/app.plainWorkflow"},                                 // registered plain ✓
		{ID: "wf-3", Name: "renamed-pipeline", ConfigName: new(string("renamed-pipeline"))}, // stranded
		{ID: "wf-4", Name: "renamed-pipeline", ConfigName: new(string("renamed-pipeline"))}, // stranded, same name
		{ID: "wf-5", Name: "gone-workflow"},                                                 // stranded, no config
	}

	stranded := strandedRuns(registered, active)

	if len(stranded) != 2 {
		t.Fatalf("stranded names = %v, want 2 entries", stranded)
	}
	if ids := stranded["renamed-pipeline/renamed-pipeline"]; len(ids) != 2 || ids[0] != "wf-3" || ids[1] != "wf-4" {
		t.Errorf("renamed-pipeline stranded IDs = %v, want [wf-3 wf-4]", ids)
	}
	if ids := stranded["gone-workflow"]; len(ids) != 1 || ids[0] != "wf-5" {
		t.Errorf("gone-workflow stranded IDs = %v, want [wf-5]", ids)
	}
}
