// Command fleet demonstrates duro's worker-pool mode: a fleet of interchangeable
// workers that recover each other's runs, and an enqueue-only web tier that
// starts work without running an engine. See the README for the walkthrough.
package main

import (
	"context"
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
const appVersion = "v1"

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
	role := flag.String("role", "worker", "worker | web")
	crash := flag.Bool("crash", false, "worker only: start a job then crash mid-run (simulates kill -9)")
	flag.Parse()

	switch *role {
	case "worker":
		runWorker(*crash)
	case "web":
		runWeb()
	default:
		fatal("unknown -role %q (want worker | web)", *role)
	}
}

// runWorker launches a worker-pool member. Fast cadences make the demo quick;
// production defaults are 10s/60s/30s.
func runWorker(crash bool) {
	app, err := duro.New(context.Background(), duro.Config{
		Name:               "fleet",
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
	fmt.Printf("[%s] graceful shutdown (tombstones the lease → immediate takeover of any undrained runs)\n", id)
	app.Shutdown(5 * time.Second)
}

// runWeb is the enqueue-only tier: it starts a run and polls its status without
// ever launching an engine.
func runWeb() {
	c, err := duro.NewClient(context.Background(), duro.ClientConfig{
		DatabaseURL: databaseURL(),
		Logger:      logger(),
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
		duro.Step("resize", func(_ context.Context, in string) (string, error) {
			for i := 1; i <= 10; i++ {
				log("resize %d/10", i)
				time.Sleep(time.Second)
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
