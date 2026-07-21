package duro_test

import (
	"context"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// --- read-side and portable messaging test workflows ------------------------

// Portable variants of the shared channels: same keys, cross-language
// serialization on the write side. Readers are unaffected — DBOS decodes by
// each value's recorded serialization.
var (
	portableNotesTopic    = duro.NewTopic[string]("notes", duro.Portable())
	portableLastItemEvent = duro.NewEvent[int]("last-item", duro.Portable())
	portableOutStream     = duro.NewStream[int]("out", duro.Portable())
)

// eventReaderWorkflow durably reads another pipeline's progress event.
func eventReaderWorkflow(ctx dbos.DBOSContext, sourceID string) (int, error) {
	return duro.Run(ctx, sourceID, duro.Pipe2(
		duro.GetEvent("read-progress", lastItemEvent, func(id string) string { return id }, 10*time.Second),
		duro.Step("double", func(_ context.Context, v int) (int, error) {
			return v * 2, nil
		}),
	))
}

// streamReaderWorkflow drains another pipeline's durable stream and folds it.
func streamReaderWorkflow(ctx dbos.DBOSContext, sourceID string) (int, error) {
	return duro.Run(ctx, sourceID, duro.Pipe2(
		duro.FromStream("drain", outStream, func(id string) string { return id },
			duro.WithTimeout(10*time.Second)),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

// portablePublishWorkflow publishes its progress event and item stream in the
// cross-language portable format.
func portablePublishWorkflow(ctx dbos.DBOSContext, ns []int) (int, error) {
	return duro.Run(ctx, ns, duro.Pipe4(
		duro.Expand("explode", explode),
		duro.SetEvent("progress", portableLastItemEvent, func(v int) int { return v }),
		duro.ToStream("emit", portableOutStream),
		duro.Reduce("sum", func(_ context.Context, acc, v int) (int, error) {
			return acc + v, nil
		}, 0),
	))
}

// portableSenderWorkflow sends a portable-format message to another
// workflow's mailbox.
func portableSenderWorkflow(ctx dbos.DBOSContext, destinationID string) (string, error) {
	return duro.Run(ctx, destinationID, duro.Pipe1(
		duro.Send("notify", portableNotesTopic, func(dest string) (string, string, error) {
			return dest, "portable ping", nil
		}),
	))
}

func registerReadSideWorkflows(ctx dbos.DBOSContext) {
	dbos.RegisterWorkflow(ctx, eventReaderWorkflow, dbos.WithWorkflowName("eventReaderWorkflow"))
	dbos.RegisterWorkflow(ctx, streamReaderWorkflow, dbos.WithWorkflowName("streamReaderWorkflow"))
	dbos.RegisterWorkflow(ctx, portablePublishWorkflow, dbos.WithWorkflowName("portablePublishWorkflow"))
	dbos.RegisterWorkflow(ctx, portableSenderWorkflow, dbos.WithWorkflowName("portableSenderWorkflow"))
}

// --- tests ------------------------------------------------------------------

// TestGetEventStage proves a pipeline can durably read an event another
// pipeline published with SetEvent, checkpointing the observed value.
func TestGetEventStage(t *testing.T) {
	_, sourceID := mustRun(t, progressWorkflow, []int{1, 2, 3})

	result, _ := mustRun(t, eventReaderWorkflow, sourceID)
	if result != 6 {
		t.Errorf("result = %d, want 6 (last-item event 3, doubled)", result)
	}
}

// TestFromStreamStage proves a pipeline can drain another pipeline's durable
// stream in one checkpointed step and process the values downstream.
func TestFromStreamStage(t *testing.T) {
	_, sourceID := mustRun(t, streamingWorkflow, []int{1, 2, 3})

	result, wfID := mustRun(t, streamReaderWorkflow, sourceID)
	if result != 6 {
		t.Errorf("result = %d, want 6 (stream values 1+2+3)", result)
	}
	// The whole drain is one checkpoint; each fold execution is one more.
	assertNames(t, stepNames(t, wfID), []string{duro.ShapeStepName, "drain", "sum", "sum", "sum"})
}

// TestPortablePublishing proves Portable() payloads written by SetEvent and
// ToStream stay readable (DBOS decodes by each value's recorded
// serialization).
func TestPortablePublishing(t *testing.T) {
	result, wfID := mustRun(t, portablePublishWorkflow, []int{1, 2, 3})
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}

	last, err := lastItemEvent.Get(app, wfID, 5*time.Second)
	if err != nil {
		t.Fatalf("reading portable event: %v", err)
	}
	if last != 3 {
		t.Errorf("last-item event = %d, want 3", last)
	}

	values, closed, err := outStream.Read(app, wfID)
	if err != nil {
		t.Fatalf("reading portable stream: %v", err)
	}
	if !closed {
		t.Errorf("stream not closed after pipeline completion")
	}
	assertInts(t, values, []int{1, 2, 3})
}

// TestPortableSend proves a Portable() message still lands in a Recv stage's
// mailbox and decodes.
func TestPortableSend(t *testing.T) {
	const receiverID = "recv-portable-ping"

	receiver, err := dbos.RunWorkflow(dctx, recvGreetingWorkflow, "", dbos.WithWorkflowID(receiverID))
	if err != nil {
		t.Fatalf("starting receiver: %v", err)
	}
	mustRun(t, portableSenderWorkflow, receiverID)

	received, err := receiver.GetResult()
	if err != nil {
		t.Fatalf("receiver failed: %v", err)
	}
	if received != "received: portable ping" {
		t.Errorf("receiver result = %q, want %q", received, "received: portable ping")
	}
}

// TestTopicSendUnblocksPipeline proves the channel's client-side Send
// delivers to a pipeline paused in a Recv stage.
func TestTopicSendUnblocksPipeline(t *testing.T) {
	const wfID = "recv-topic-send"

	handle, err := dbos.RunWorkflow(dctx, recvGreetingWorkflow, "", dbos.WithWorkflowID(wfID))
	if err != nil {
		t.Fatalf("starting workflow: %v", err)
	}
	if err := notesTopic.Send(app, wfID, "typed hello"); err != nil {
		t.Fatalf("sending via topic: %v", err)
	}
	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("workflow failed: %v", err)
	}
	if result != "received: typed hello" {
		t.Errorf("result = %q, want %q", result, "received: typed hello")
	}
}
