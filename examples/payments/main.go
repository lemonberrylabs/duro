package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"time"

	"github.com/lemonberrylabs/duro"
)

func main() {
	app, err := duro.New(context.Background(), duro.Config{
		Name:        "duro-payments",
		DatabaseURL: databaseURL(),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal("initializing: %v", err)
	}

	fast := duro.Register(app, "fast-payment", FastPayment)
	reviewed := duro.Register(app, "reviewed-payment", ReviewedPayment)
	audit := duro.Register(app, "audit", Audit)

	if err := app.Launch(); err != nil {
		fatal("launching: %v", err)
	}
	defer app.Shutdown(5 * time.Second)

	// 1. A small payment sails through — but watch the risk-check retries.
	section("fast payment (retries + backoff + durable settle delay)")
	fastHandle, err := fast.Start(app, Payment{ID: "p-100", Card: "4242", AmountCents: 4_500})
	if err != nil {
		fatal("starting fast payment: %v", err)
	}
	receipt, err := fastHandle.Result()
	if err != nil {
		fatal("fast payment failed: %v", err)
	}
	fmt.Printf("   receipt: %+v\n", receipt)
	fmt.Printf("   risk-check attempts: %d (two transient failures retried)\n", riskAttempts.Load())

	// 2. A large payment pauses for a human. We watch its progress event,
	//    then approve it over the typed topic — from plain client code.
	section("reviewed payment (Recv pause + Topic approval + Event progress)")
	reviewedHandle, err := reviewed.Start(app, Payment{ID: "p-200", Card: "4242", AmountCents: 250_000})
	if err != nil {
		fatal("starting reviewed payment: %v", err)
	}
	phase, err := Progress.Get(app, reviewedHandle.ID(), 10*time.Second)
	if err != nil {
		fatal("reading progress: %v", err)
	}
	fmt.Printf("   progress event: %q — sending approval\n", phase)
	if err := Approvals.Send(app, reviewedHandle.ID(), Approval{PaymentID: "p-200", ApprovedBy: "demo"}); err != nil {
		fatal("approving: %v", err)
	}
	receipt, err = reviewedHandle.Result()
	if err != nil {
		fatal("reviewed payment failed: %v", err)
	}
	fmt.Printf("   receipt: %+v\n", receipt)

	// 3. A declined card fails once — the retry predicate refuses to burn
	//    retries on a permanent error.
	section("declined payment (retry predicate stops permanent errors)")
	attemptsBefore := riskAttempts.Load()
	declinedHandle, err := fast.Start(app, Payment{ID: "p-300", Card: "0000", AmountCents: 1_000})
	if err != nil {
		fatal("starting declined payment: %v", err)
	}
	if _, err := declinedHandle.Result(); err != nil {
		fmt.Printf("   failed as expected: %v\n", err)
	}
	fmt.Printf("   risk-check attempts since: %d (validate failed first, nothing retried)\n", riskAttempts.Load()-attemptsBefore)

	// 4. The auditor pipeline drains the fast payment's receipt stream —
	//    reading another workflow's stream as one checkpointed step.
	section("audit (FromStream + client-side Stream.Read)")
	auditHandle, err := audit.Start(app, fastHandle.ID())
	if err != nil {
		fatal("starting audit: %v", err)
	}
	entries, err := auditHandle.Result()
	if err != nil {
		fatal("audit failed: %v", err)
	}
	for _, e := range entries {
		fmt.Printf("   audited %s: %s\n", e.PaymentID, e.Note)
	}

	// The same stream, read from plain client code via the channel value.
	values, closed, err := Receipts.Read(app, reviewedHandle.ID())
	if err != nil {
		fatal("reading receipts: %v", err)
	}
	fmt.Printf("   client read of reviewed payment's stream: %d receipt(s), closed=%v\n", len(values), closed)
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_payments", username)
}

func section(title string) {
	fmt.Printf("\n── %s\n", title)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
