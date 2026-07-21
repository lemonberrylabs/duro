// Package main demonstrates duro's operational toolkit: cron pipelines
// (RegisterScheduled), burst-collapsing (RegisterDebounced), surgical replay
// of a finished run from a named stage (ForkFromStage), and the durable
// identity contract behind pipeline names (the stranded-run warning).
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lemonberrylabs/duro"
)

// --- scheduled: a digest that runs on a cron -------------------------------
// Scheduled pipelines take time.Time — the tick — as input; the compiler
// enforces it (Pipeline[time.Time, R]).

var digestTicks atomic.Int64

var DigestPipeline = duro.Pipe2(
	duro.Step("gather", func(_ context.Context, tick time.Time) (int, error) {
		digestTicks.Add(1)
		return int(tick.Unix() % 100), nil // the "metrics query"
	}),
	duro.Step("publish", func(_ context.Context, activeUsers int) (string, error) {
		return fmt.Sprintf("digest: %d active users", activeUsers), nil
	}),
)

// --- debounced: reindex storms collapse to one run -------------------------

var (
	reindexRuns atomic.Int64
	reindexed   atomic.Value // []string: the last input that actually ran
)

var ReindexPipeline = duro.Pipe2(
	duro.Step("plan", func(_ context.Context, docIDs []string) ([]string, error) {
		reindexRuns.Add(1)
		return docIDs, nil
	}),
	duro.Step("index", func(_ context.Context, docIDs []string) (int, error) {
		reindexed.Store(docIDs)
		return len(docIDs), nil
	}),
)

// --- forkable: an expensive fetch, then a formatting stage with a "bug" ----
// formatBroken simulates deploying buggy code and then fixing it: the demo
// runs the pipeline with the bug, flips the flag (the "fixed deploy"), and
// forks the finished run from the "format" stage. The expensive fetch
// replays from its checkpoint — it does not run again.

var (
	fetchRuns    atomic.Int64
	formatBroken atomic.Bool
)

var ReportPipeline = duro.Pipe3(
	duro.Step("fetch", func(_ context.Context, quarter string) ([]int, error) {
		fetchRuns.Add(1)
		time.Sleep(time.Second) // the expensive part
		return []int{12, 7, 42}, nil
	}),
	duro.Step("format", func(_ context.Context, figures []int) (string, error) {
		if formatBroken.Load() {
			return fmt.Sprintf("%v", figures[:1]), nil // the bug: drops figures
		}
		return fmt.Sprintf("%v", figures), nil
	}),
	duro.Step("publish-report", func(_ context.Context, body string) (string, error) {
		return "report " + body, nil
	}),
)

// --- durable identity: a pipeline that waits for a signal ------------------
// The stranded-run demo starts a Welcome run (it parks in Recv), exits, and
// relaunches under a different registered name: app.Launch() warns that the
// in-flight run's name is no longer registered. See README for the 3-run arc.

var Welcomes = duro.NewTopic[string]("welcomes")

var WelcomePipeline = duro.Pipe2(
	duro.Recv[string]("await-name", Welcomes, 10*time.Minute),
	duro.Step("greet", func(_ context.Context, name string) (string, error) {
		return "welcome, " + name, nil
	}),
)
