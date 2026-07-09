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

// Send durably sends one message per item to another workflow's mailbox on
// the given topic (dbos.Send is checkpointed, so a recovered workflow does
// not re-send). fn derives the destination workflow ID and the message from
// the item, which passes through unchanged.
func Send[T, M any](name, topic string, fn func(in T) (destinationID string, message M, err error)) Stage[T, T] {
	mustValidStage("Send", name, fn == nil)
	return Stage[T, T]{name: name, kind: "send", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (T, context.Context, error) {
		state, err := stageState(ctx, name)
		if err != nil {
			return in, ctx, err
		}
		destinationID, message, err := fn(in)
		if err == nil {
			err = dbos.Send(state.dctx, destinationID, message, topic)
		}
		if err != nil {
			state.aborted.Store(true)
			return in, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return in, ctx, nil
	})}
}

// Recv durably waits for the next message of type M on the given topic and
// emits it downstream, consuming one message per upstream item (the item
// itself is discarded — reshape beforehand if you need it). Receipt is
// checkpointed, so a recovered workflow does not consume a second message.
// A zero or negative timeout means dbos.Recv's no-wait behavior; if no
// message arrives in time, the stage fails the pipeline.
//
// Recv is how a pipeline pauses for an external signal — a payment
// confirmation, a human approval — sent with dbos.Send from anywhere (another
// workflow or a plain client) to this workflow's ID.
func Recv[T, M any](name, topic string, timeout time.Duration) Stage[T, M] {
	if name == "" {
		panic("duro: Recv stage requires a non-empty name")
	}
	return Stage[T, M]{name: name, kind: "recv", apply: ro.MapErrWithContext(func(ctx context.Context, _ T) (M, context.Context, error) {
		var zero M
		state, err := stageState(ctx, name)
		if err != nil {
			return zero, ctx, err
		}
		message, err := dbos.Recv[M](state.dctx, topic, timeout)
		if err != nil {
			state.aborted.Store(true)
			return zero, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return message, ctx, nil
	})}
}

// SetEvent durably publishes a key-value event on the workflow for each item
// and passes the item through unchanged. Clients (or other workflows) read it
// with dbos.GetEvent — the classic use is exposing pipeline progress to the
// outside world while the workflow runs.
func SetEvent[T, V any](name, key string, fn func(in T) V) Stage[T, T] {
	mustValidStage("SetEvent", name, fn == nil)
	if key == "" {
		panic(fmt.Sprintf("duro: SetEvent stage %q requires a non-empty key", name))
	}
	return Stage[T, T]{name: name, kind: "set-event", apply: ro.MapErrWithContext(func(ctx context.Context, in T) (T, context.Context, error) {
		state, err := stageState(ctx, name)
		if err != nil {
			return in, ctx, err
		}
		if err := dbos.SetEvent(state.dctx, key, fn(in)); err != nil {
			state.aborted.Store(true)
			return in, ctx, fmt.Errorf("duro: stage %q: %w", name, err)
		}
		return in, ctx, nil
	})}
}

// ToStream durably appends each item to the workflow's named stream and
// passes it through unchanged; the stream is closed when the pipeline
// completes. Readers consume it incrementally with dbos.ReadStream — the way
// to expose a pipeline's per-item output while it is still running, instead
// of waiting for the final result.
func ToStream[T any](name, key string) Stage[T, T] {
	if name == "" {
		panic("duro: ToStream stage requires a non-empty name")
	}
	if key == "" {
		panic(fmt.Sprintf("duro: ToStream stage %q requires a non-empty stream key", name))
	}
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
					if err := dbos.WriteStream(state.dctx, key, in); err != nil {
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: writing stream %q: %w", name, key, err))
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
					if err := dbos.CloseStream(state.dctx, key); err != nil {
						state.aborted.Store(true)
						fail(ctx, fmt.Errorf("duro: stage %q: closing stream %q: %w", name, key, err))
						return
					}
					dest.CompleteWithContext(ctx)
				},
			))

			return sub.Unsubscribe
		})
	}}
}
