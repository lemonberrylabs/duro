// Package main demonstrates duro's control-flow combinators on a support
// ticket triage pipeline: Switch dispatches by category, a nested Branch
// escalates urgent bugs, Loop durably polls an external system, Sub reuses a
// shared notification segment, and Collect folds the batch into a report —
// all inside one registered pipeline, no hand-written workflow function.
package main

import (
	"context"
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

// billing: refund, then the shared notify segment.
var billingArm = duro.Pipe2(
	duro.Step("refund", func(_ context.Context, t Ticket) (Ticket, error) {
		return t, nil // call your billing provider here
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

// --- the pipeline ------------------------------------------------------------

var TriagePipeline = duro.Pipe3(
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
	duro.Collect[Resolution]("report"),
)
