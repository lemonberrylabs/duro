package duro

// Shape-fingerprint regression guard.
//
// The fingerprint is checkpointed as the hidden "duro.shape" step and compared
// on every replay, so its exact text is duro's on-disk format: if a refactor
// changes one byte, every in-flight run built by the previous binary fails its
// shape check on recovery and can never complete. The golden strings below
// were captured from the released code, so this test fails if the rendering
// ever drifts — including through the typed stageKind constants and the
// recursive nested-shape rendering, neither of which may alter the output.
// Shared fingerprint corpus. Compiled into both the pre-change tree (to emit
// golden values) and the post-change tree (to assert them).

import (
	"context"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

func fpStep(name string) Stage[int, int] {
	return Step(name, func(_ context.Context, v int) (int, error) { return v, nil })
}

func fpChild(_ dbos.DBOSContext, v int) (int, error) { return v, nil }

var fpQueue = NewQueue("fp-corpus-queue")

type fpCase struct {
	label string
	fp    string
}

// fpCorpus returns one entry per representative shape, labelled so a mismatch
// names the construct that drifted.
func fpCorpus() []fpCase {
	leaf := Pipe2(fpStep("leaf-a"), fpStep("leaf-b"))
	inner := Pipe1(fpStep("inner"))

	return []fpCase{
		{"linear-all-leaf-kinds", Pipe8(
			fpStep("s"),
			Tap("t", func(_ context.Context, _ int) error { return nil }),
			Filter("f", func(_ context.Context, _ int) (bool, error) { return true, nil }),
			Expand("e", func(_ context.Context, v int) ([]int, error) { return []int{v}, nil }),
			Pure("p", func(v int) int { return v }),
			Delay[int]("d", time.Second),
			Parallel("par", 2, func(_ context.Context, v int) (int, error) { return v, nil }),
			Reduce("r", func(_ context.Context, a, v int) (int, error) { return a + v, nil }, 0),
		).fingerprint()},

		{"collect", Pipe2(fpStep("s"), Collect[int]("c")).fingerprint()},

		{"branch", Pipe1(Branch("b",
			func(_ context.Context, _ int) (bool, error) { return true, nil },
			leaf, inner,
		)).fingerprint()},

		{"switch-three-cases", Pipe1(Switch("sw",
			func(_ context.Context, _ int) (string, error) { return "x", nil },
			When("x", leaf), When("y", inner), When("z", Pipe1(fpStep("z-only"))),
		)).fingerprint()},

		{"loop", Pipe1(Loop("lp", inner,
			func(_ context.Context, _ int) (bool, error) { return true, nil },
		)).fingerprint()},

		{"rescue", Pipe1(Rescue("rs", leaf,
			func(_ context.Context, v int, _ error) (int, error) { return v, nil },
		)).fingerprint()},

		{"sub", Pipe1(Sub("sb", leaf)).fingerprint()},

		{"via", Pipe1(Via("v", leaf)).fingerprint()},

		{"fanout", Pipe3(
			Expand("x", func(_ context.Context, v int) ([]int, error) { return []int{v}, nil }),
			FanOut("fo", fpQueue, Workflow(fpChild)),
			Reduce("sum", func(_ context.Context, a, v int) (int, error) { return a + v, nil }, 0),
		).fingerprint()},

		{"fanout-cancel-siblings", Pipe3(
			Expand("x", func(_ context.Context, v int) ([]int, error) { return []int{v}, nil }),
			FanOut("fo", fpQueue, Workflow(fpChild), WithCancelSiblings()),
			Reduce("sum", func(_ context.Context, a, v int) (int, error) { return a + v, nil }, 0),
		).fingerprint()},

		// Nesting inside nesting: a Branch arm that itself contains a Switch
		// containing a Sub. This is the case the recursive rendering had to
		// reproduce exactly.
		{"deeply-nested", Pipe1(Branch("outer",
			func(_ context.Context, _ int) (bool, error) { return true, nil },
			Pipe1(Switch("mid",
				func(_ context.Context, _ int) (string, error) { return "a", nil },
				When("a", Pipe1(Sub("deep", leaf))),
				When("b", Pipe1(Via("deep-via", inner))),
			)),
			Pipe1(Loop("alt", inner, func(_ context.Context, _ int) (bool, error) { return true, nil })),
		)).fingerprint()},
	}
}

// goldenFingerprints is keyed by the corpus label. Captured from the code as
// released; never edit a value to make a test pass — a diff here means a
// durability-breaking change, not a stale expectation.
var goldenFingerprints = map[string]string{
	"linear-all-leaf-kinds":  "step:s → tap:t → filter:f → expand:e → pure:p → delay:d → parallel:par → reduce:r",
	"collect":                "step:s → collect:c",
	"branch":                 "branch:b[then=step:leaf-a → step:leaf-b | else=step:inner]",
	"switch-three-cases":     "switch:sw[x=step:leaf-a → step:leaf-b | y=step:inner | z=step:z-only]",
	"loop":                   "loop:lp[step:inner]",
	"rescue":                 "rescue:rs[step:leaf-a → step:leaf-b]",
	"sub":                    "sub:sb[step:leaf-a → step:leaf-b]",
	"via":                    "via:v[step:leaf-a → step:leaf-b]",
	"fanout":                 "expand:x → fanout:fo → reduce:sum",
	"fanout-cancel-siblings": "expand:x → fanout+cancel:fo → reduce:sum",
	"deeply-nested":          "branch:outer[then=switch:mid[a=sub:deep[step:leaf-a → step:leaf-b] | b=via:deep-via[step:inner]] | else=loop:alt[step:inner]]",
}

func TestShapeFingerprintIsUnchanged(t *testing.T) {
	corpus := fpCorpus()
	if len(corpus) != len(goldenFingerprints) {
		t.Fatalf("corpus has %d shapes but %d golden values — add the new shape's golden value", len(corpus), len(goldenFingerprints))
	}
	for _, c := range corpus {
		want, ok := goldenFingerprints[c.label]
		if !ok {
			t.Errorf("no golden fingerprint for %q", c.label)
			continue
		}
		if c.fp != want {
			t.Errorf("fingerprint drift for %s:\n got: %s\nwant: %s", c.label, c.fp, want)
		}
	}
}

// TestShapeFingerprintDistinguishesNestedEdits proves the guard still bites
// where it must: editing an embedded pipeline changes the enclosing stage's
// fingerprint, so a Branch arm cannot be swapped under an in-flight run.
func TestShapeFingerprintDistinguishesNestedEdits(t *testing.T) {
	pred := func(_ context.Context, _ int) (bool, error) { return true, nil }
	base := Pipe1(Branch("b", pred, Pipe1(fpStep("x")), Pipe1(fpStep("y")))).fingerprint()
	edited := Pipe1(Branch("b", pred, Pipe1(fpStep("x-edited")), Pipe1(fpStep("y")))).fingerprint()
	if base == edited {
		t.Error("editing a Branch arm left the fingerprint unchanged — the shape guard would miss the change")
	}
}
