# housekeeping — cron, debounce, forking, and durable identity

The operational side of duro: pipelines that run themselves on a schedule,
collapse bursts into single runs, get surgically replayed after a bad deploy,
and what the *name* of a pipeline really means.

```bash
createdb duro_housekeeping
go run .    # or: DBOS_SYSTEM_DATABASE_URL=... go run .
```

## What it demonstrates

| Feature | Where |
|---|---|
| `RegisterScheduled` | `DigestPipeline` runs every 2 seconds; its input is the tick time, and `Pipeline[time.Time, R]` makes that a compile-time rule |
| `RegisterDebounced` + `Debounce` | five reindex triggers in a burst → one durable run with the last trigger's input; every call returns a handle to the same eventual run |
| `ForkFromStage` | a report formatted by buggy code is re-run from the `"format"` stage after the "fix" — the expensive `fetch` **replays from its checkpoint** (the counter proves it never re-executes) |
| `Handle.Status` | the forked run's record carries `ForkedFrom` |
| `duro.WithWorkflowID` | the welcome run gets a caller-chosen ID so a later process can signal it |
| Stranded-run detection | the three-phase demo below |

## The durable-identity arc

A pipeline's registered name is its **durable identity**: in-flight runs are
recovered by looking the name up at launch. Rename a pipeline between deploys
and its old runs can never recover. duro turns that silent failure into a
launch-time warning — walk it end to end:

```bash
go run . -stranded=start     # 1. park a run in Recv, then close the app
go run . -stranded=renamed   # 2. register the pipeline as "welcome-v2" instead
go run . -stranded=reattach  # 3. register "welcome" again and signal the run
```

Phase 2 prints, from `app.Launch()`:

```
WARN duro: in-flight workflows are recorded under a name that is no longer
     registered and cannot recover on this executor — was the pipeline renamed?
     workflow_name=welcome/welcome count=1 workflow_ids=[welcome-visitor]
```

Phase 3 re-registers the original name: recovery re-attaches the parked run,
`Welcomes.Send` delivers the signal it was waiting for, and it completes.
(The arc is one-shot per database — the reattached run is finished; `dropdb`/
`createdb` to repeat it.)

Two details worth internalizing:

- **Phase 1 closes gracefully.** Since DBOS v1.1, `app.Close` cancels local
  execution so it can unwind but does not durably cancel the workflow. The
  parked `Recv` remains PENDING and is recoverable on the next launch. Use
  `duro.Cancel` when the durable intent is cancellation; process shutdown now
  means interruption and recovery.
- **The warning is advisory.** Launch does not fail; the stranded runs stay
  in the database untouched. Re-registering the old name (phase 3) or
  `dbos.ForkWorkflow` onto current code are both recovery paths.

## The fork-after-fix pattern

`ReportPipeline`'s `format` stage has a simulated bug behind a flag. The demo
runs it (wrong output), flips the flag — standing in for deploying fixed
code — and calls:

```go
duro.ForkFromStage[string](app, duro.Fork{WorkflowID: buggy.ID(), Stage: "format"})
```

Stages before `format` replay from the original run's checkpoints; `format`
and everything after re-execute on the fixed code. In production the fix is a
real deploy, and `Fork.ApplicationVersion` pins the forked run to it.
