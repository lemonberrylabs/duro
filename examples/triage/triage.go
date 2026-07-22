// Package main demonstrates duro's control-flow combinators on a support
// ticket triage pipeline: Switch dispatches by category, a nested Branch
// escalates urgent bugs, Loop durably polls an external system, Sub reuses a
// shared notification segment, Rescue scopes error handling to a best-effort
// segment (and to the whole pipeline), Via fans each resolution out to
// archive systems and passes the resolution through, and Collect folds the
// batch into a report — all inside one registered pipeline, no hand-written
// workflow function.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lemonberrylabs/duro"
)

// --- domain types ------------------------------------------------------------

type Ticket struct {
	ID       string
	Category string // "billing" | "bug" | "spam"
	Urgent   bool
	Attempts int // simulated fix-service polls consumed so far
}

type Resolution struct {
	TicketID string
	Outcome  string
}

// pollCalls counts fix-service polls so the demo can show Loop's durable
// iteration count.
var pollCalls atomic.Int64

// loyaltyCalls counts loyalty-service attempts so the demo can show the
// retry envelope exhausting before the Rescue handler fires — and staying
// exhausted on replay.
var loyaltyCalls atomic.Int64

// --- shared segment (Sub) ----------------------------------------------------
// notify is one pipeline segment reused by two Switch arms — embedded with
// Sub, it stays a single named unit in each arm's shape.

func notify(outcome string) duro.Pipeline[Ticket, Resolution] {
	return duro.Pipe2(
		duro.Tap("email-customer", func(_ context.Context, t Ticket) error {
			fmt.Printf("      [notify] %s: %s\n", t.ID, outcome)
			return nil
		}),
		duro.Pure("resolve", func(t Ticket) Resolution {
			return Resolution{TicketID: t.ID, Outcome: outcome}
		}),
	)
}

// --- the arms ----------------------------------------------------------------

// billing: refund, then a best-effort goodwill credit, then the shared
// notify segment. The loyalty service is down, so the credit step burns its
// retries and fails — and Rescue scopes fail-fast away from the segment: the
// handler (itself a checkpointed step, so the decision replays) swallows the
// terminal failure and the refund still notifies. Retry-then-swallow without
// a hand-rolled retry loop.
var loyaltyCredit = duro.Pipe1(
	duro.Step("loyalty-credit", func(_ context.Context, t Ticket) (Ticket, error) {
		loyaltyCalls.Add(1)
		return t, errors.New("loyalty service: 503")
	}, duro.WithMaxRetries(2), duro.WithBaseInterval(50*time.Millisecond)),
)

var billingArm = duro.Pipe3(
	duro.Step("refund", func(_ context.Context, t Ticket) (Ticket, error) {
		return t, nil // call your billing provider here
	}),
	duro.Rescue("credit-best-effort", loyaltyCredit,
		func(_ context.Context, t Ticket, cause error) (Ticket, error) {
			fmt.Printf("      [rescue] %s: skipping loyalty credit: %v\n", t.ID, cause)
			return t, nil // pass-through swallow: a credit failure must not block the refund
		}),
	duro.Sub("notify-refund", notify("refunded")),
)

// bug: urgent bugs escalate and durably poll the fix service until it
// reports fixed; routine bugs just get filed. The Branch nests inside the
// Switch arm — control flow composes.
var bugArm = duro.Pipe1(
	duro.Branch("urgency", func(_ context.Context, t Ticket) (bool, error) {
		return t.Urgent, nil
	},
		duro.Pipe3(
			duro.Tap("page-oncall", func(_ context.Context, t Ticket) error {
				fmt.Printf("      [escalate] %s: paging on-call\n", t.ID)
				return nil
			}),
			duro.Loop("await-fix",
				duro.Pipe2(
					duro.Delay[Ticket]("backoff", 100*time.Millisecond), // durable pause between polls
					duro.Step("poll-fix", func(_ context.Context, t Ticket) (Ticket, error) {
						pollCalls.Add(1)
						t.Attempts++
						return t, nil
					}),
				),
				func(_ context.Context, t Ticket) (bool, error) {
					return t.Attempts >= 3, nil // the "fix service" reports fixed on the third poll
				},
			),
			duro.Sub("notify-fixed", notify("fixed")),
		),
		duro.Pipe1(duro.Sub("notify-filed", notify("filed"))),
	),
)

// spam: close without ceremony.
var spamArm = duro.Pipe1(
	duro.Pure("close", func(t Ticket) Resolution {
		return Resolution{TicketID: t.ID, Outcome: "closed as spam"}
	}),
)

// --- the archive fan-out (Via) -----------------------------------------------
// Every resolution is archived to two compliance systems, each filing a child
// workflow on a queue. The filings' outcomes are not the stream — the
// resolution is: Via runs the embedded fan-out for its effects and passes the
// resolution through to the report. Before Via, this shape was the last
// reason to hand-write a workflow function: run the fan-out with RunAll,
// discard its results, continue with the value you had before it.

// ArchiveJob is the FanOut child input: one filing per (system, resolution).
type ArchiveJob struct {
	System   string
	TicketID string
	Outcome  string
}

var archiveQueue = duro.NewQueue("triage-archive", duro.WithConcurrency(2))

// archiveRuns counts child filings so the demo can show the fan-out ran (and
// replays for free).
var archiveRuns atomic.Int64

// ArchiveResolution is the FanOut child workflow: one durable filing per
// system. Registered in main.go; referenced here via duro.Workflow.
func ArchiveResolution(_ duro.Context, job ArchiveJob) (string, error) {
	archiveRuns.Add(1)
	return job.System + ":" + job.TicketID, nil
}

var archiveTrail = duro.Pipe2(
	duro.Expand("systems", func(_ context.Context, r Resolution) ([]ArchiveJob, error) {
		return []ArchiveJob{
			{System: "warehouse", TicketID: r.TicketID, Outcome: r.Outcome},
			{System: "legal-hold", TicketID: r.TicketID, Outcome: r.Outcome},
		}, nil
	}),
	duro.FanOut("file", archiveQueue, duro.Workflow(ArchiveResolution)),
)

// --- the pipeline ------------------------------------------------------------

var TriagePipeline = duro.Pipe4(
	duro.Expand("explode", func(_ context.Context, ts []Ticket) ([]Ticket, error) {
		return ts, nil
	}),
	duro.Switch("dispatch", func(_ context.Context, t Ticket) (string, error) {
		return t.Category, nil
	},
		duro.When("billing", billingArm),
		duro.When("bug", bugArm),
		duro.When("spam", spamArm),
	),
	duro.Via("archive-trail", archiveTrail),
	duro.Collect[Resolution]("report"),
)

// GuardedTriagePipeline wraps the whole triage in a top-level except block:
// on any failure, report it where operators will see it (a progress doc, a
// status page), then rethrow the cause unchanged — the run still fails, but
// never silently. Because the handler is checkpointed, the report is not
// re-sent if a recovered run replays the failure.
var GuardedTriagePipeline = duro.Pipe1(
	duro.Rescue("run", TriagePipeline,
		func(_ context.Context, batch []Ticket, cause error) ([]Resolution, error) {
			fmt.Printf("      [report] triage of %d tickets failed: %v\n", len(batch), cause)
			return nil, cause // rethrow: a reported failure is still a failure
		}),
)
