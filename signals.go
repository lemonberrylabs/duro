package duro

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/samber/ro"
)

// Delay is a durable pause: each item passing through sleeps for d via
// dbos.Sleep, which checkpoints the wake-up deadline — a workflow recovered
// mid-sleep resumes sleeping only for the remaining time, and a replayed
// sleep completes instantly. Use it for pacing between durable stages;
// remember it applies per item on multi-item streams.
func Delay[T any](name string, d time.Duration) Stage[T, T] {
	if name == "" {
		panic("duro: Delay stage requires a non-empty name")
	}
	if d <= 0 {
		panic(fmt.Sprintf("duro: Delay stage %q requires a positive duration", name))
	}
	return Stage[T, T]{name: name, kind: "delay", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (T, context.Context, error) {
		state, err := stageState(ctx, name)
		if err != nil {
			return in, ctx, err
		}
		if _, err := dbos.Sleep(state.dctx, d); err != nil {
			state.aborted.Store(true)
			return in, ctx, fmt.Errorf("duro: stage %q: durable sleep: %w", name, err)
		}
		return in, ctx, nil
	})}
}

// Send durably sends one message per item on the topic (dbos.Send is
// checkpointed, so a recovered workflow does not re-send). fn derives the
// destination workflow ID and the message from the item, which passes
// through unchanged. The message type is the topic's — a mismatch with the
// receiving side is a compile error.
func Send[T, M any](name string, topic Topic[M], fn func(in T) (destinationID string, message M, err error)) Stage[T, T] {
	mustValidStage("Send", name, fn == nil)
	mustValidChannel("Send", name, topic.name)
	return Stage[T, T]{name: name, kind: "send", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (T, context.Context, error) {
		state, err := stageState(ctx, name)
		if err != nil {
			return in, ctx, err
		}
		destinationID, message, err := fn(in)
		if err == nil {
			err = dbos.Send(state.dctx, destinationID, message, topic.name, topic.sendOptions()...)
		}
		if err != nil {
			state.aborted.Store(true)
			return in, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return in, ctx, nil
	})}
}

// Recv durably waits for the next message on the topic and emits it
// downstream, consuming one message per upstream item (the item itself is
// discarded — reshape beforehand if you need it). Receipt is checkpointed,
// so a recovered workflow does not consume a second message. A zero or
// negative timeout means dbos.Recv's no-wait behavior; if no message arrives
// in time, the stage fails the pipeline.
//
// Recv is how a pipeline pauses for an external signal — a payment
// confirmation, a human approval — sent to this workflow's ID with a Send
// stage or Topic.Send from anywhere.
func Recv[T, M any](name string, topic Topic[M], timeout time.Duration) Stage[T, M] {
	if name == "" {
		panic("duro: Recv stage requires a non-empty name")
	}
	mustValidChannel("Recv", name, topic.name)
	return Stage[T, M]{name: name, kind: "recv", apply: ro.MapErrWithContext(func(ctx context.Context, _ T) (M, context.Context, error) {
		var zero M
		state, err := stageState(ctx, name)
		if err != nil {
			return zero, ctx, err
		}
		message, err := dbos.Recv[M](state.dctx, topic.name, timeout)
		if err != nil {
			state.aborted.Store(true)
			return zero, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return message, ctx, nil
	})}
}

// SetEvent durably publishes the event on the workflow for each item and
// passes the item through unchanged. Read it with a GetEvent stage or
// Event.Get — the classic use is exposing pipeline progress to the outside
// world while the workflow runs.
func SetEvent[T, V any](name string, event Event[V], fn func(in T) V) Stage[T, T] {
	mustValidStage("SetEvent", name, fn == nil)
	mustValidChannel("SetEvent", name, event.key)
	return Stage[T, T]{name: name, kind: "set-event", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (T, context.Context, error) {
		state, err := stageState(ctx, name)
		if err != nil {
			return in, ctx, err
		}
		if err := dbos.SetEvent(state.dctx, event.key, fn(in), event.setOptions()...); err != nil {
			state.aborted.Store(true)
			return in, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return in, ctx, nil
	})}
}

// GetEvent durably reads the event published by another workflow: fn derives
// the source workflow ID from the item, and the event's value is emitted
// downstream in place of the item (reshape beforehand if you need both). The
// read blocks until the event is set or the timeout elapses, and is
// checkpointed — a recovered workflow replays the value it already observed
// instead of re-reading.
func GetEvent[T, V any](name string, event Event[V], fn func(in T) (workflowID string), timeout time.Duration) Stage[T, V] {
	mustValidStage("GetEvent", name, fn == nil)
	mustValidChannel("GetEvent", name, event.key)
	return Stage[T, V]{name: name, kind: "get-event", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (V, context.Context, error) {
		var zero V
		state, err := stageState(ctx, name)
		if err != nil {
			return zero, ctx, err
		}
		value, err := dbos.GetEvent[V](state.dctx, fn(in), event.key, timeout)
		if err != nil {
			state.aborted.Store(true)
			return zero, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return value, ctx, nil
	})}
}

// ToStream durably appends each item to the workflow's stream and passes it
// through unchanged; the stream is closed when the pipeline completes.
// Readers drain it with a FromStream stage or Stream.Read — the way to
// expose a pipeline's per-item output while it is still running, instead of
// waiting for the final result. The stream's type is the pipeline's item
// type, checked at compile time.
func ToStream[T any](name string, stream Stream[T]) Stage[T, T] {
	if name == "" {
		panic("duro: ToStream stage requires a non-empty name")
	}
	mustValidChannel("ToStream", name, stream.key)
	writeOpts := stream.writeOptions()
	return Stage[T, T]{name: name, kind: "to-stream", apply: func(source ro.Observable[T]) ro.Observable[T] {
		return ro.NewUnsafeObservableWithContext(func(subCtx context.Context, dest ro.Observer[T]) ro.Teardown {
			failed := false

			fail := func(ctx context.Context, err error) {
				failed = true
				dest.ErrorWithContext(ctx, err)
			}

			sub := source.SubscribeWithContext(subCtx, ro.NewObserverWithContext(
				func(ctx context.Context, in T) {
					if failed {
						return
					}
					state, err := stageState(ctx, name)
					if err != nil {
						fail(ctx, err)
						return
					}
					if err := dbos.WriteStream(state.dctx, stream.key, in, writeOpts...); err != nil {
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: writing stream %q: %w", name, stream.key, err))
						return
					}
					dest.NextWithContext(ctx, in)
				},
				dest.ErrorWithContext,
				func(ctx context.Context) {
					if failed {
						return
					}
					state, err := stageState(ctx, name)
					if err != nil {
						fail(ctx, err)
						return
					}
					if err := dbos.CloseStream(state.dctx, stream.key); err != nil {
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: closing stream %q: %w", name, stream.key, err))
						return
					}
					dest.CompleteWithContext(ctx)
				},
			))

			return sub.Unsubscribe
		})
	}}
}

// FromStream durably drains the stream written by another workflow: fn
// derives the source workflow ID from the item, the stream is read to its
// close (blocking while the writer is still active), and each value is
// emitted downstream in order — the item itself is discarded, like Recv.
// The whole read runs as one checkpointed step, so a recovered workflow
// replays the values it already collected instead of re-reading a stream
// that may have changed. Pair it with WithTimeout to bound how long the
// stage waits for the writer to finish.
func FromStream[T, V any](name string, stream Stream[V], fn func(in T) (workflowID string), opts ...StepOption) Stage[T, V] {
	mustValidStage("FromStream", name, fn == nil)
	mustValidChannel("FromStream", name, stream.key)
	return Stage[T, V]{name: name, kind: "from-stream", apply: ro.FlatMapWithContext(func(ctx context.Context, in T) ro.Observable[V] {
		state, err := stageState(ctx, name)
		if err != nil {
			return ro.Throw[V](err)
		}
		values, err := runStep(ctx, name, in, func(stepCtx context.Context, in T) ([]V, error) {
			return collectStream[V](stepCtx, state.dctx, fn(in), stream.key)
		}, opts)
		if err != nil {
			return ro.Throw[V](err)
		}
		return ro.FromSlice(values)
	})}
}

// collectStream drains a durable stream into a slice, honoring the step
// context's deadline and cancellation.
func collectStream[V any](stepCtx context.Context, dctx dbos.DBOSContext, workflowID, key string) ([]V, error) {
	ch, err := dbos.ReadStreamAsync[V](dctx, workflowID, key)
	if err != nil {
		return nil, err
	}
	var values []V
	for {
		select {
		case sv, ok := <-ch:
			if !ok || sv.Closed {
				return values, nil
			}
			if sv.Err != nil {
				return nil, sv.Err
			}
			values = append(values, sv.Value)
		case <-stepCtx.Done():
			return nil, stepCtx.Err()
		}
	}
}

// mustValidChannel panics when a stage references a zero-value channel — the
// channel must come from NewTopic/NewEvent/NewStream.
func mustValidChannel(kind, name, channelName string) {
	if channelName == "" {
		panic(fmt.Sprintf("duro: %s stage %q requires a channel built by NewTopic/NewEvent/NewStream", kind, name))
	}
}
