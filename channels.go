package duro

import (
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Channels pair the two sides of DBOS messaging by construction. A Topic,
// Event, or Stream is declared once (package level is fine) and referenced by
// both writer and reader, so the identifying key lives in one place and the
// payload type is checked by the compiler — sending an Approval and receiving
// an Invoice is a compile error, not a runtime decode failure.

// ChannelOption configures a declared channel.
type ChannelOption func(*channelConfig)

type channelConfig struct {
	portable bool
}

// Portable makes every payload written through the channel serialize in
// DBOS's cross-language portable JSON format, so non-Go DBOS applications
// (Python, TypeScript) can consume it. Readers need nothing special — DBOS
// decodes by each value's recorded serialization.
func Portable() ChannelOption {
	return func(c *channelConfig) { c.portable = true }
}

func newChannelConfig(kind, name string, opts []ChannelOption) channelConfig {
	if name == "" {
		panic(fmt.Sprintf("duro: %s requires a non-empty name", kind))
	}
	var cfg channelConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Topic is a typed mailbox channel: messages of type M sent to a workflow's
// mailbox under one topic name. Write with a Send stage or Topic.Send; read
// with a Recv stage.
//
//	var Approvals = duro.NewTopic[Approval]("approvals")
type Topic[M any] struct {
	name string
	cfg  channelConfig
}

// NewTopic declares a typed mailbox topic.
func NewTopic[M any](name string, opts ...ChannelOption) Topic[M] {
	return Topic[M]{name: name, cfg: newChannelConfig("NewTopic", name, opts)}
}

// Name returns the topic's name.
func (t Topic[M]) Name() string { return t.name }

// Send delivers a message to the destination workflow's mailbox on this
// topic. It works from anywhere: inside a workflow it is checkpointed (no
// re-send on replay); from a plain client it is a direct send. Pipelines
// should prefer the Send stage.
func (t Topic[M]) Send(ctx Context, destinationID string, message M) error {
	return dbos.Send(unwrapContext(ctx), destinationID, message, t.name, t.sendOptions()...)
}

func (t Topic[M]) sendOptions() []dbos.SendOption {
	if t.cfg.portable {
		return []dbos.SendOption{dbos.WithPortableSend()}
	}
	return nil
}

// Event is a typed key-value event channel: a value of type V published on a
// workflow under one key. Write with a SetEvent stage; read with a GetEvent
// stage or Event.Get.
//
//	var Progress = duro.NewEvent[int]("last-item")
type Event[V any] struct {
	key string
	cfg channelConfig
}

// NewEvent declares a typed event key.
func NewEvent[V any](key string, opts ...ChannelOption) Event[V] {
	return Event[V]{key: key, cfg: newChannelConfig("NewEvent", key, opts)}
}

// Key returns the event's key.
func (e Event[V]) Key() string { return e.key }

// Get reads the event's value from the given workflow, blocking until it is
// set or the timeout elapses. Inside a workflow the read is checkpointed;
// from a plain client it is a direct read. Pipelines should prefer the
// GetEvent stage.
func (e Event[V]) Get(ctx Context, workflowID string, timeout time.Duration) (V, error) {
	return dbos.GetEvent[V](unwrapContext(ctx), workflowID, e.key, timeout)
}

func (e Event[V]) setOptions() []dbos.SetEventOption {
	if e.cfg.portable {
		return []dbos.SetEventOption{dbos.WithPortableSetEvent()}
	}
	return nil
}

// Stream is a typed durable stream channel: values of type V appended by one
// workflow and drained by readers. Write with a ToStream stage; read with a
// FromStream stage or Stream.Read.
//
//	var Receipts = duro.NewStream[Receipt]("receipts")
type Stream[V any] struct {
	key string
	cfg channelConfig
}

// NewStream declares a typed stream key.
func NewStream[V any](key string, opts ...ChannelOption) Stream[V] {
	return Stream[V]{key: key, cfg: newChannelConfig("NewStream", key, opts)}
}

// Key returns the stream's key.
func (s Stream[V]) Key() string { return s.key }

// Read drains the stream written by the given workflow, blocking until the
// writer closes it or becomes inactive; closed reports whether the stream was
// cleanly closed. Read is for clients — inside a pipeline use the FromStream
// stage, which checkpoints the read so replay never observes a changed
// stream.
func (s Stream[V]) Read(ctx Context, workflowID string) (values []V, closed bool, err error) {
	return dbos.ReadStream[V](unwrapContext(ctx), workflowID, s.key)
}

func (s Stream[V]) writeOptions() []dbos.WriteStreamOption {
	if s.cfg.portable {
		return []dbos.WriteStreamOption{dbos.WithPortableWriteStream()}
	}
	return nil
}
