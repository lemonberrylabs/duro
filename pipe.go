package duro

import (
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
	kind   string
	name   string
	nested []string // fingerprints of embedded pipelines, part of the shape
}

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
func (p Pipeline[P, R]) fingerprint() string {
	parts := make([]string, len(p.shape))
	for i, s := range p.shape {
		parts[i] = s.kind + ":" + s.name
		if len(s.nested) > 0 {
			parts[i] += "[" + strings.Join(s.nested, " | ") + "]"
		}
	}
	return strings.Join(parts, " → ")
}

// Pipe1 composes a pipeline from 1 stage.
func Pipe1[A, B any](s1 Stage[A, B]) Pipeline[A, B] {
	return Pipeline[A, B]{
		shape:  []stageInfo{s1.info()},
		queues: concatQueues(s1.queues),
		apply:  s1.apply,
	}
}

// Pipe2 composes a pipeline from 2 stages.
func Pipe2[A, B, C any](s1 Stage[A, B], s2 Stage[B, C]) Pipeline[A, C] {
	return Pipeline[A, C]{
		shape:  []stageInfo{s1.info(), s2.info()},
		queues: concatQueues(s1.queues, s2.queues),
		apply: func(src ro.Observable[A]) ro.Observable[C] {
			return s2.apply(s1.apply(src))
		},
	}
}

// Pipe3 composes a pipeline from 3 stages.
func Pipe3[A, B, C, D any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D]) Pipeline[A, D] {
	return Pipeline[A, D]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues),
		apply: func(src ro.Observable[A]) ro.Observable[D] {
			return s3.apply(s2.apply(s1.apply(src)))
		},
	}
}

// Pipe4 composes a pipeline from 4 stages.
func Pipe4[A, B, C, D, E any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E]) Pipeline[A, E] {
	return Pipeline[A, E]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info(), s4.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues, s4.queues),
		apply: func(src ro.Observable[A]) ro.Observable[E] {
			return s4.apply(s3.apply(s2.apply(s1.apply(src))))
		},
	}
}

// Pipe5 composes a pipeline from 5 stages.
func Pipe5[A, B, C, D, E, F any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F]) Pipeline[A, F] {
	return Pipeline[A, F]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues),
		apply: func(src ro.Observable[A]) ro.Observable[F] {
			return s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src)))))
		},
	}
}

// Pipe6 composes a pipeline from 6 stages.
func Pipe6[A, B, C, D, E, F, G any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G]) Pipeline[A, G] {
	return Pipeline[A, G]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues),
		apply: func(src ro.Observable[A]) ro.Observable[G] {
			return s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src))))))
		},
	}
}

// Pipe7 composes a pipeline from 7 stages.
func Pipe7[A, B, C, D, E, F, G, H any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G], s7 Stage[G, H]) Pipeline[A, H] {
	return Pipeline[A, H]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info(), s7.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues, s7.queues),
		apply: func(src ro.Observable[A]) ro.Observable[H] {
			return s7.apply(s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src)))))))
		},
	}
}

// Pipe8 composes a pipeline from 8 stages.
func Pipe8[A, B, C, D, E, F, G, H, I any](s1 Stage[A, B], s2 Stage[B, C], s3 Stage[C, D], s4 Stage[D, E], s5 Stage[E, F], s6 Stage[F, G], s7 Stage[G, H], s8 Stage[H, I]) Pipeline[A, I] {
	return Pipeline[A, I]{
		shape:  []stageInfo{s1.info(), s2.info(), s3.info(), s4.info(), s5.info(), s6.info(), s7.info(), s8.info()},
		queues: concatQueues(s1.queues, s2.queues, s3.queues, s4.queues, s5.queues, s6.queues, s7.queues, s8.queues),
		apply: func(src ro.Observable[A]) ro.Observable[I] {
			return s8.apply(s7.apply(s6.apply(s5.apply(s4.apply(s3.apply(s2.apply(s1.apply(src))))))))
		},
	}
}
