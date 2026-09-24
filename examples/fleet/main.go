// Command fleet demonstrates duro's worker-pool mode: a fleet of interchangeable
// workers that recover each other's runs, an enqueue-only web tier that starts
// work without running an engine, and an admin view on that same client that
// lists, inspects, cancels, and resumes runs, and checks whether a run is
// settled. See the README for the walkthrough.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"os/user"
	"syscall"
	"time"

	"github.com/lemonberrylabs/duro"
)

// appVersion pins recovery: only workers on this version take over each other's
// runs. In production this would be a git SHA or release tag.
const (
	appName    = "fleet"
	appVersion = "v1"
)

// resizeQueue is the queue the web tier enqueues onto and every worker listens
// on. The concurrency cap is what makes takeover's queue handling visible: an
// adopted run is re-enqueued on this queue, not shunted onto DBOS's internal
// one, so a crash cannot let the fleet exceed the limit set here.
var resizeQueue = duro.NewQueue("resize-jobs", duro.WithConcurrency(2))

// resizeJob is the pipeline's cross-process identity: the workers register it
// and the web tier enqueues it, so the name and both types come from this one
// declaration instead of a string repeated in two binaries.
var resizeJob = duro.NewJob[string, string]("resize")

func main() {
	role := flag.String("role", "worker", "worker | web | admin")
	crash := flag.Bool("crash", false, "worker only: start a job then crash mid-run (simulates kill -9)")
	cancelID := flag.String("cancel", "", "admin only: cancel the run with this ID before listing")
	resumeID := flag.String("resume", "", "admin only: resume the run with this ID before listing (retries-exceeded, or cancelled before it started)")
	settledID := flag.String("settled", "", "admin only: report whether the run with this ID is final and none of its code is still running")
	flag.Parse()

	switch *role {
	case "worker":
		runWorker(*crash)
	case "web":
		runWeb()
	case "admin":
		runAdmin(*cancelID, *resumeID, *settledID)
	default:
		fatal("unknown -role %q (want worker | web | admin)", *role)
	}
}

// runWorker launches a worker-pool member. Fast cadences make the demo quick;
// production defaults are 10s/60s/30s.
func runWorker(crash bool) {
	app, err := duro.New(context.Background(), duro.Config{
		Name:               appName,
		DatabaseURL:        databaseURL(),
		ApplicationVersion: appVersion,
		Logger:             logger(),
	}, duro.WithWorkerPool(
		duro.WithHeartbeatInterval(time.Second),
		duro.WithStaleThreshold(5*time.Second),
	),
		duro.WithSweepInterval(2*time.Second),
		duro.WithStaleRunWarning(15*time.Second))
	if err != nil {
		fatal("initializing worker: %v", err)
	}
	if err := duro.RegisterQueues(app, resizeQueue); err != nil {
		fatal("registering queue: %v", err)
	}
	id := app.Context().GetExecutorID()
	wf := duro.RegisterJob(app, resizeJob, resizePipeline(id))
	if err := app.Launch(); err != nil {
		fatal("launching worker: %v", err)
	}
	fmt.Printf("[%s] worker up (version %s)\n", id, appVersion)

	if crash {
		h, err := wf.Start(app, "beach.jpg")
		if err != nil {
			fatal("starting run: %v", err)
		}
		fmt.Printf("[%s] started run %s — crashing mid-resize\n", id, h.ID())
		time.Sleep(3 * time.Second) // let it reach the slow resize step
		fmt.Printf("[%s] 💥 kill -9: no graceful shutdown, lease left to go stale\n", id)
		os.Exit(1)
	}

	fmt.Printf("[%s] running jobs and sweeping for dead workers; Ctrl-C to stop\n", id)
	waitForSignal()
	fmt.Printf("[%s] graceful shutdown (tombstones the lease → immediate takeover of interrupted runs)\n", id)
	app.Close(5 * time.Second)
}

// runWeb is the enqueue-only tier: it starts a run and polls its status without
// ever launching an engine.
func runWeb() {
	c, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL:     databaseURL(),
		ApplicationName: appName,
		Logger:          logger(),
	})
	if err != nil {
		fatal("initializing client: %v", err)
	}
	defer c.Shutdown(5 * time.Second)

	h, err := duro.Enqueue(c, resizeQueue, resizeJob, "vacation.jpg")
	if err != nil {
		fatal("enqueuing: %v", err)
	}
	fmt.Printf("[web] enqueued run %s (no engine here — a worker will run it)\n", h.ID())

	for {
		s, err := c.Status(h.ID())
		if err != nil {
			fatal("checking status: %v", err)
		}
		fmt.Printf("[web] status: %s\n", s.State)
		if s.State.Terminal() {
			result, err := h.Result()
			if err != nil {
				fatal("run failed: %v", err)
			}
			fmt.Printf("[web] result: %s\n", result)
			return
		}
		time.Sleep(time.Second)
	}
}

// runAdmin is the operator's view, built on the same enqueue-only client as the
// web tier: remediate a run if asked, then list every resize run in the system
// — whatever process ran it — and dump the newest one's checkpoints. Nothing
// here needs the pipeline registered; the client reads through the same
// mapping the workers use, so the states it prints are the workers' states.
func runAdmin(cancelID, resumeID, settledID string) {
	c, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL:     databaseURL(),
		ApplicationName: appName,
		Logger:          logger(),
		// Every read below fails after 10s instead of hanging for as long as
		// the database is unreachable (the default bound is 30s).
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		fatal("initializing client: %v", err)
	}
	defer c.Shutdown(5 * time.Second)

	if cancelID != "" {
		remediate("cancel", cancelID, c.Cancel(cancelID))
	}
	if resumeID != "" {
		remediate("resume", resumeID, c.Resume(resumeID))
	}
	if settledID != "" {
		reportSettled(c, settledID)
	}

	runs, err := c.ListRuns(
		duro.WithNames(resizeJob.Name()),
		duro.WithNewestFirst(),
		duro.WithLimit(10),
		duro.WithInput(), // off by default: the list stays payload-free unless asked
	)
	if err != nil {
		fatal("listing runs: %v", err)
	}
	if len(runs) == 0 {
		fmt.Println("[admin] no resize runs yet — start one with -role=web")
		return
	}
	fmt.Printf("[admin] newest %d %s runs:\n", len(runs), resizeJob.Name())
	for _, r := range runs {
		fmt.Printf("[admin]   %s  %-16s attempts=%d queue=%-12q executor=%s input=%s\n",
			r.ID, r.State, r.Attempts, r.QueueName, r.ExecutorID, r.Input)
		if r.Err != nil {
			fmt.Printf("[admin]     error: %v\n", r.Err)
		}
	}

	latest := runs[0]
	steps, err := c.Steps(latest.ID)
	if err != nil {
		fatal("listing steps: %v", err)
	}
	fmt.Printf("[admin] checkpoints of %s (%s):\n", latest.ID, latest.State)
	for _, s := range steps {
		fmt.Printf("[admin]   %d  %-12s %s → %s\n", s.ID, s.Name,
			s.StartedAt.Format("15:04:05.000"), s.CompletedAt.Format("15:04:05.000"))
		if s.Err != nil {
			fmt.Printf("[admin]      error: %v\n", s.Err)
		}
	}
}

// remediate reports a Cancel/Resume outcome. The typed errors are the point:
// neither call silently no-ops, so an admin button can say exactly why nothing
// changed.
func remediate(what, id string, err error) {
	switch {
	case err == nil:
		fmt.Printf("[admin] %s %s: done\n", what, id)
	case errors.Is(err, duro.ErrRunNotFound):
		fatal("%s %s: no such run", what, id)
	case errors.Is(err, duro.ErrRunTerminal):
		fmt.Printf("[admin] %s %s: refused, run already finished (%v)\n", what, id, err)
	case errors.Is(err, duro.ErrRunActive):
		fmt.Printf("[admin] %s %s: refused, run is still live — cancel it first (%v)\n", what, id, err)
	case errors.Is(err, duro.ErrRunInFlight):
		fmt.Printf("[admin] %s %s: refused, the run was cancelled mid-stage and that stage may still be running — re-run it with ForkFromStage on a worker (%v)\n", what, id, err)
	default:
		fatal("%s %s: %v", what, id, err)
	}
}

// reportSettled says whether a run is final with none of its code running —
// the check to make before starting work that must not overlap it. A run
// cancelled mid-stage is final at once, but unsettled until the worker's
// stage has actually returned.
func reportSettled(c *duro.Client, id string) {
	runs, err := c.Unsettled(id)
	switch {
	case errors.Is(err, duro.ErrRunNotFound):
		fatal("settled %s: no such run", id)
	case err != nil:
		fatal("settled %s: %v", id, err)
	case len(runs) == 0:
		fmt.Printf("[admin] %s is settled: final, and none of its code is running\n", id)
	case runs[0].Executing:
		fmt.Printf("[admin] %s is %s and a worker may still be executing it\n", id, runs[0].State)
	default:
		fmt.Printf("[admin] %s is %s: not final yet\n", id, runs[0].State)
	}
}

// resizePipeline is a fake image resize: decode → resize (slow) → encode. Each
// step logs the executor that ran it, so after a takeover you can see the
// checkpointed decode step replay (no re-log) while the survivor re-runs the
// in-flight resize.
func resizePipeline(execID string) duro.Pipeline[string, string] {
	log := func(format string, a ...any) { fmt.Printf("[%s] "+format+"\n", append([]any{execID}, a...)...) }
	return duro.Pipe3(
		duro.Step("decode", func(_ context.Context, name string) (string, error) {
			log("decode %s", name)
			return "decoded:" + name, nil
		}),
		// resize honors its context: cancelling the run interrupts it
		// within about a heartbeat interval instead of letting it finish.
		duro.Step("resize", func(ctx context.Context, in string) (string, error) {
			for i := 1; i <= 10; i++ {
				log("resize %d/10", i)
				select {
				case <-ctx.Done():
					log("resize interrupted: %v", context.Cause(ctx))
					return "", ctx.Err()
				case <-time.After(time.Second):
				}
			}
			return "resized:" + in, nil
		}),
		duro.Step("encode", func(_ context.Context, in string) (string, error) {
			log("encode")
			return "done:" + in, nil
		}),
	)
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_fleet", username)
}

func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
