// Package main demonstrates duro's signal and resilience toolkit on a
// payment flow: retries with a predicate, per-attempt timeouts, durable
// pauses, human-in-the-loop approval over a typed Topic, progress Events,
// and a receipt Stream drained by a second pipeline.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lemonberrylabs/duro"
)

// --- domain types ------------------------------------------------------------

type Payment struct {
	ID          string
	Card        string
	AmountCents int
}

type Approval struct {
	PaymentID  string
	ApprovedBy string
}

type Receipt struct {
	PaymentID   string
	AmountCents int
	ChargeID    string
	ApprovedBy  string // empty for auto-approved payments
}

type AuditEntry struct {
	PaymentID string
	Note      string
}

// ErrCardDeclined is a permanent failure: retrying cannot fix it.
var ErrCardDeclined = errors.New("card declined")

// --- typed channels ----------------------------------------------------------
// Declared once; every writer and reader references these values, so the key
// lives in one place and the payload type is checked by the compiler.

var (
	// Approvals carries human decisions into paused pipelines.
	Approvals = duro.NewTopic[Approval]("approvals")
	// Progress exposes each payment's current phase while it runs.
	Progress = duro.NewEvent[string]("progress")
	// Receipts is each payment run's output stream. Portable: a Python or
	// TypeScript DBOS app could consume it as JSON.
	Receipts = duro.NewStream[Receipt]("receipts", duro.Portable())
)

// --- a tiny "database" -------------------------------------------------------
// Recv emits the received message and discards the in-flight item, so a
// pipeline that pauses for approval persists its state first (here a map; in
// production, your database) and reloads it when the signal arrives.

var pendingPayments sync.Map // payment ID → Payment

// riskAttempts counts risk-service calls to show retries in the demo output.
var riskAttempts atomic.Int64

// --- stages ------------------------------------------------------------------
// Stages are values: both pipelines below are assembled from this shared set.

// validate fails permanently on a declined card: the retry predicate stops
// the retry budget from being wasted on an error that cannot succeed.
var validate = duro.Step("validate", func(_ context.Context, p Payment) (Payment, error) {
	if strings.HasSuffix(p.Card, "0000") {
		return Payment{}, fmt.Errorf("payment %s: %w", p.ID, ErrCardDeclined)
	}
	if p.AmountCents <= 0 {
		return Payment{}, fmt.Errorf("payment %s: amount must be positive", p.ID)
	}
	return p, nil
},
	duro.WithMaxRetries(3),
	duro.WithRetryPredicate(func(err error) bool { return !errors.Is(err, ErrCardDeclined) }),
)

// riskCheck simulates a flaky, occasionally slow scoring service: transient
// errors retry with exponential backoff, and each attempt is bounded by a
// deadline so a hung call cannot stall the pipeline.
var riskCheck = duro.Step("risk-check", func(stepCtx context.Context, p Payment) (Payment, error) {
	if riskAttempts.Add(1)%3 != 0 { // two transient failures, then success
		return Payment{}, errors.New("risk service unavailable (transient)")
	}
	select { // the "call", honoring the per-attempt deadline
	case <-time.After(50 * time.Millisecond):
		return p, nil
	case <-stepCtx.Done():
		return Payment{}, stepCtx.Err()
	}
},
	duro.WithMaxRetries(5),
	duro.WithBaseInterval(50*time.Millisecond),
	duro.WithBackoffFactor(2),
	duro.WithMaxInterval(time.Second),
	duro.WithTimeout(2*time.Second),
)

// persist records the payment so the approval path can reload it after Recv.
var persist = duro.Tap("persist", func(_ context.Context, p Payment) error {
	pendingPayments.Store(p.ID, p)
	return nil
})

// phase publishes the pipeline's current phase on the Progress event.
func phase(name, value string) duro.Stage[Payment, Payment] {
	return duro.SetEvent(name, Progress, func(Payment) string { return value })
}

// charge produces the receipt; approvedBy tags who released the payment.
func charge(approvedBy func(Payment) string) duro.Stage[Payment, Receipt] {
	return duro.Step("charge", func(_ context.Context, p Payment) (Receipt, error) {
		return Receipt{
			PaymentID:   p.ID,
			AmountCents: p.AmountCents,
			ChargeID:    "ch_" + p.ID,
			ApprovedBy:  approvedBy(p),
		}, nil
	})
}

// --- pipelines ---------------------------------------------------------------

// FastPayment: validate → score → charge → settle, no human involved.
var FastPayment = duro.Pipe6(
	validate,
	riskCheck,
	phase("phase-charging", "charging"),
	charge(func(Payment) string { return "" }),
	duro.Delay[Receipt]("settle", time.Second), // durable pause: survives restarts
	duro.ToStream("receipt", Receipts),
)

// ReviewedPayment pauses for a human: it persists the payment, parks in Recv
// until an Approval arrives on the Approvals topic, reloads the payment, and
// only then charges.
var ReviewedPayment = duro.Pipe8(
	validate,
	riskCheck,
	persist,
	phase("phase-waiting", "awaiting-approval"),
	duro.Recv[Payment]("await-approval", Approvals, 30*time.Second),
	duro.Step("reload", func(_ context.Context, a Approval) (Payment, error) {
		p, ok := pendingPayments.Load(a.PaymentID)
		if !ok {
			return Payment{}, fmt.Errorf("no pending payment %s", a.PaymentID)
		}
		pendingPayments.Delete(a.PaymentID)
		return p.(Payment), nil
	}),
	charge(func(p Payment) string { return "reviewer" }),
	duro.ToStream("receipt", Receipts),
)

// Audit drains a finished payment run's receipt stream (input: its workflow
// ID) and reads its final progress event — the read-side stages. FromStream
// is one checkpointed step, so a recovered auditor replays the values it saw.
var Audit = duro.Pipe3(
	duro.FromStream("drain-receipts", Receipts, func(paymentRunID string) string { return paymentRunID },
		duro.WithTimeout(10*time.Second)),
	duro.Pure("to-entry", func(r Receipt) AuditEntry {
		return AuditEntry{PaymentID: r.PaymentID, Note: fmt.Sprintf("charged %d¢ (%s)", r.AmountCents, r.ChargeID)}
	}),
	duro.Reduce("collect", func(_ context.Context, acc []AuditEntry, e AuditEntry) ([]AuditEntry, error) {
		return append(acc, e), nil
	}, nil),
)
