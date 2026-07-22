package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"time"

	"github.com/lemonberrylabs/duro"
)

const welcomeRunID = "welcome-visitor"

func main() {
	stranded := flag.String("stranded", "", "durable-identity demo phase: start | renamed | reattach (see README)")
	flag.Parse()

	app, err := duro.New(context.Background(), duro.Config{
		Name:        "duro-housekeeping",
		DatabaseURL: databaseURL(),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal("initializing: %v", err)
	}

	// The welcome pipeline's registered name is its durable identity. The
	// "renamed" phase registers it under a different name to show what
	// app.Launch() says about the in-flight run recorded under the old one.
	welcomeName := "welcome"
	if *stranded == "renamed" {
		welcomeName = "welcome-v2"
	}
	welcome := duro.Register(app, welcomeName, WelcomePipeline)

	// Every-2-seconds cron so the demo sees ticks quickly; a real schedule
	// would be something like "0 0 6 * * *" (daily at 06:00, with seconds).
	duro.RegisterScheduled(app, "digest", "*/2 * * * * *", DigestPipeline)
	reindex := duro.RegisterDebounced(app, "reindex", ReindexPipeline)
	report := duro.Register(app, "quarterly-report", ReportPipeline)

	if err := app.Launch(); err != nil {
		fatal("launching: %v", err)
	}
	defer app.Shutdown(5 * time.Second)

	switch *stranded {
	case "start":
		if _, err := welcome.Start(app, "", duro.WithWorkflowID(welcomeRunID)); err != nil {
			fatal("starting welcome run: %v", err)
		}
		fmt.Printf("started %q (parked in Recv) — exiting like a crash so it stays PENDING\n", welcomeRunID)
		fmt.Println("now run with -stranded=renamed")
		os.Exit(0) // skip the deferred Shutdown: a graceful drain would abort the parked Recv
	case "renamed":
		fmt.Println("registered the pipeline as \"welcome-v2\" — the warning above is app.Launch()")
		fmt.Println("finding the in-flight run recorded under \"welcome\". Run -stranded=reattach to fix it.")
		return
	case "reattach":
		if err := Welcomes.Send(app, welcomeRunID, "ada"); err != nil {
			fatal("sending welcome: %v", err)
		}
		fmt.Println("re-registered \"welcome\": recovery re-attached the parked run; sent it a name")
		time.Sleep(2 * time.Second) // let the recovered run finish
		return
	}

	// 1. Scheduled pipeline: cron ticks arrive as durable runs.
	section("scheduled digest (*/2s cron, input is the tick time)")
	time.Sleep(4500 * time.Millisecond)
	fmt.Printf("   digest runs so far: %d (each one a durable workflow)\n", digestTicks.Load())

	// 2. Debounced pipeline: five rapid triggers, one run, last input wins.
	section("debounced reindex (5 triggers in a burst → 1 run)")
	var last duro.Handle[int]
	for i := 1; i <= 5; i++ {
		docs := make([]string, i) // each trigger has a bigger doc set
		for j := range docs {
			docs[j] = fmt.Sprintf("doc-%d", j+1)
		}
		if last, err = reindex.Debounce(app, "catalog", 800*time.Millisecond, docs); err != nil {
			fatal("debouncing: %v", err)
		}
	}
	indexed, err := last.Result()
	if err != nil {
		fatal("reindex failed: %v", err)
	}
	fmt.Printf("   pipeline executions: %d, indexed %d docs (the last trigger's input)\n", reindexRuns.Load(), indexed)

	// 3. Fork from a stage: fix the bug, replay the run from "format" —
	//    the expensive fetch replays from its checkpoint.
	section("fork from a stage (replay a finished run on fixed code)")
	formatBroken.Store(true)
	buggy, err := report.Start(app, "Q2-2026")
	if err != nil {
		fatal("starting report: %v", err)
	}
	buggyResult, err := buggy.Result()
	if err != nil {
		fatal("report failed: %v", err)
	}
	fmt.Printf("   with the bug:  %q (fetch executions: %d)\n", buggyResult, fetchRuns.Load())

	formatBroken.Store(false) // the "fixed deploy"
	fixed, err := duro.ForkFromStage[string](app, duro.Fork{
		WorkflowID: buggy.ID(),
		Stage:      "format",
	})
	if err != nil {
		fatal("forking: %v", err)
	}
	fixedResult, err := fixed.Result()
	if err != nil {
		fatal("forked report failed: %v", err)
	}
	status, err := fixed.Status()
	if err != nil {
		fatal("forked status: %v", err)
	}
	fmt.Printf("   after forking: %q (fetch executions: %d — replayed, not re-run)\n", fixedResult, fetchRuns.Load())
	fmt.Printf("   forked from:   %s\n", status.ForkedFrom)
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_housekeeping", username)
}

func section(title string) {
	fmt.Printf("\n── %s\n", title)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
