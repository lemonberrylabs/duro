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
		Name:        "duro-triage",
		DatabaseURL: databaseURL(),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal("initializing: %v", err)
	}

	duro.RegisterWorkflow(app, "ArchiveResolution", ArchiveResolution)
	triage := duro.Register(app, "triage", TriagePipeline)
	guarded := duro.Register(app, "triage-guarded", GuardedTriagePipeline)

	if err := app.Launch(); err != nil {
		fatal("launching: %v", err)
	}
	defer app.Shutdown(5 * time.Second)

	batch := []Ticket{
		{ID: "t-1", Category: "billing"},
		{ID: "t-2", Category: "bug", Urgent: true},
		{ID: "t-3", Category: "spam"},
		{ID: "t-4", Category: "bug"},
	}

	section("triage run (Switch → nested Branch → Loop → Rescue → Via, one durable workflow)")
	handle, err := triage.Start(app, batch)
	if err != nil {
		fatal("starting triage: %v", err)
	}
	report, err := handle.Result()
	if err != nil {
		fatal("triage failed: %v", err)
	}
	for _, r := range report {
		fmt.Printf("   %s → %s\n", r.TicketID, r.Outcome)
	}
	fmt.Printf("   fix-service polls: %d (the urgent bug looped until fixed)\n", pollCalls.Load())
	fmt.Printf("   loyalty attempts: %d (retries exhausted, then rescued — the refund survived)\n", loyaltyCalls.Load())
	fmt.Printf("   archive filings: %d child workflows (Via ran the fan-out; the report still carries resolutions)\n", archiveRuns.Load())

	// Every routing decision, loop iteration, and rescue decision is a
	// checkpoint: re-running the same workflow ID replays the whole triage
	// from Postgres.
	section("replay (same run ID: decisions replay, nothing re-executes)")
	beforePolls, beforeLoyalty, beforeArchives := pollCalls.Load(), loyaltyCalls.Load(), archiveRuns.Load()
	replayed, err := triage.Start(app, batch, duro.WithWorkflowID(handle.ID()))
	if err != nil {
		fatal("replaying: %v", err)
	}
	if _, err := replayed.Result(); err != nil {
		fatal("replay failed: %v", err)
	}
	fmt.Printf("   fix-service polls during replay: %d, loyalty attempts: %d, archive filings: %d\n",
		pollCalls.Load()-beforePolls, loyaltyCalls.Load()-beforeLoyalty, archiveRuns.Load()-beforeArchives)

	// A ticket with an unknown category fails the Switch. The guarded
	// pipeline's top-level Rescue reports the failure, then rethrows it —
	// the whole-pipeline except block.
	section("whole-pipeline except block (report, then rethrow)")
	poisoned, err := guarded.Start(app, []Ticket{{ID: "t-9", Category: "mystery"}})
	if err != nil {
		fatal("starting guarded triage: %v", err)
	}
	if _, err := poisoned.Result(); err != nil {
		fmt.Printf("   run failed as it should: %v\n", err)
	} else {
		fatal("guarded triage unexpectedly succeeded")
	}
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_triage", username)
}

func section(title string) {
	fmt.Printf("\n── %s\n", title)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
