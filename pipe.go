package duro

import (
	"fmt"
	"strings"

	"github.com/samber/ro"
)

// Pipeline is a composed chain of stages from input P to result R. Pipelines
// are immutable and stateless: build them once (package level is fine) and
// run them from any workflow with Run or RunAll.
type Pipeline[P, R any] struct {
	shape  []stageInfo
	apply  func(ro.Observable[P]) ro.Observable[R]
	queues []Queue // every queue the pipeline's stages enqueue onto
}

type stageInfo struct {
	kind   stageKind
	name   string
	nested []nestedShape // embedded pipelines' shapes, part of this stage's shape
}

// nestedShape is one pipeline embedded in a control-flow stage. key labels a
// Switch or Branch arm and is empty for the single-body stages (Loop, Rescue,
// Sub, Via), which mirrors how each renders into the fingerprint.
type nestedShape struct {
	key   string
	shape []stageInfo
}

// Fingerprint rendering. These separators are part of the recorded shape
// checkpoint that Run compares on every replay, so they are duro's on-disk
// format: changing one invalidates the shape of every in-flight run.
const (
	fingerprintKindNameSep = ":"
	fingerprintStageSep    = " → "
	fingerprintNestedOpen  = "["
	fingerprintNestedClose = "]"
	fingerprintNestedSep   = " | "
	fingerprintCaseKeySep  = "="
)

// concatQueues flattens the queues referenced by a pipeline's stages.
func concatQueues(qs ...[]Queue) []Queue {
	var out []Queue
	for _, q := range qs {
		out = append(out, q...)
	}
	return out
}

// fingerprint is the pipeline's shape identity, checkpointed by Run as the
// hidden "duro.shape" step and compared on replay. Control-flow stages fold
// their embedded pipelines' fingerprints in, so editing a Branch arm or Loop
// body still trips the shape guard.
func (p Pipeline[P, R]) fingerprint() string { return fingerprintOf(p.shape) }

// fingerprintOf renders a shape, recursing through embedded pipelines.
func fingerprintOf(shape []stageInfo) string {
	parts := make([]string, len(shape))
	for i, s := range shape {
		parts[i] = string(s.kind) + fingerprintKindNameSep + s.name
		if len(s.nested) == 0 {
			continue
		}
		inner := make([]string, len(s.nested))
		for j, n := range s.nested {
			inner[j] = fingerprintOf(n.shape)
			if n.key != "" {
				inner[j] = n.key + fingerprintCaseKeySep + inner[j]
			}
		}
		parts[i] += fingerprintNestedOpen + strings.Join(inner, fingerprintNestedSep) + fingerprintNestedClose
	}
	return strings.Join(parts, fingerprintStageSep)
}

// mustPipeline assembles a validated pipeline from its composed stages. Stage
// names must be unique within the pipeline they are piped into: a name is how
// a stage is identified in the recorded step list, so two stages sharing one
// makes ForkFromStage ambiguous — it would silently target whichever ran
// first. Embedded pipelines are validated the same way when they are built,
// and arms of the same Branch or Switch may reuse names freely because only
// one arm ever runs.
func mustPipeline[P, R any](shape []stageInfo, queues []Queue, apply func(ro.Observable[P]) ro.Observable[R]) Pipeline[P, R] {
	seen := make(map[string]stageKind, len(shape))
	for _, s := range shape {
		if prior, dup := seen[s.name]; dup {
			panic(fmt.Sprintf("duro: pipeline has two stages named %q (%s and %s) — stage names identify a stage in the recorded step list, so they must be unique within a pipeline", s.name, prior, s.kind))
		}
		seen[s.name] = s.kind
	}
	return Pipeline[P, R]{shape: shape, queues: queues, apply: apply}
}

// Pipe1 composes a pipeline from 1 stage.
func Pipe1[A, B any](s1 Stage[A, B]) Pipeline[A, B] {
	return mustPipeline[A, B](
		[]stageInfo{s1.info()},
		concatQueues(s1.queues),
		s1.apply,
	)
}

// Pipe2 composes a pipeline from 2 stages.
func Pipe2[A, B, C any](s1 Stage[A, B], s2 Stage[B, C]) Pipeline[A, C] {
	return mustPipeline[A, C](
		[]stageInfo{s1.info(), s2.info()},
		concatQueues(s1.queues, s2.queues),
		func(src ro.Observable[A]) ro.Observable[C] {
			return s2.apply(s1.apply(src))
		},
	)
}

// Pipe3 composes a pipeline from 3 stages.
func Pipe3[A, B, C, D any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D]) Pipeline[A, D] {
	return mustPipeline[A, D](
		[]stageInfo{s1.info(), s2.info(), s3.info()},
		concatQueues(s1.queues, s2.queues, s3.queues),
		func(src ro.Observable[A]) ro.Observable[D] {
			return s3.apply(s2.apply(s1.apply(src)))
		},
	)
}

// Pipe4 composes a pipeline from 4 stages.
func Pipe4[A, B, C, D, E any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E]) Pipeline[A, E] {
	return mustPipeline[A, E](
		[]stageInfo{s1.info(), s2.info(), s3.info(), s4.info()},
		concatQueues(s1.queues, s2.queues, s3.queues, s4.queues),
		func(src ro.Observable[A]) ro.Observable[E] {
			return s4.apply(s3.apply(s2.apply(s1.apply(src))))
		},
	)
}

// Pipe5 composes a pipeline from 5 stages.
func Pipe5[A, B, C, D, E, F any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F]) Pipeline[A, F] {
	return mustPipeline[A, F](
		[]stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info()},
		concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues),
		func(src ro.Observable[A]) ro.Observable[F] {
			return s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src)))))
		},
	)
}

// Pipe6 composes a pipeline from 6 stages.
func Pipe6[A, B, C, D, E, F, G any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G]) Pipeline[A, G] {
	return mustPipeline[A, G](
		[]stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info()},
		concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues),
		func(src ro.Observable[A]) ro.Observable[G] {
			return s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src))))))
		},
	)
}

// Pipe7 composes a pipeline from 7 stages.
func Pipe7[A, B, C, D, E, F, G, H any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G], s7 Stage[G, H]) Pipeline[A, H] {
	return mustPipeline[A, H](
		[]stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info(), s7.info()},
		concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues, s7.queues),
		func(src ro.Observable[A]) ro.Observable[H] {
			return s7.apply(s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src)))))))
		},
	)
}

// Pipe8 composes a pipeline from 8 stages.
func Pipe8[A, B, C, D, E, F, G, H, I any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G], s7 Stage[G, H], s8 Stage[H, I]) Pipeline[A, I] {
	return mustPipeline[A, I](
		[]stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info(), s7.info(), s8.info()},
		concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues, s7.queues, s8.queues),
		func(src ro.Observable[A]) ro.Observable[I] {
			return s8.apply(s7.apply(s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src))))))))
		},
	)
}
