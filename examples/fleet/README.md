# fleet — worker-pool mode

The infrastructure side of duro: a fleet of interchangeable workers that recover
each other's runs on ephemeral hardware (no DBOS Conductor), and an enqueue-only
web tier that starts work without running an engine.

```bash
createdb duro_fleet
go run . -role=worker          # a worker  (or: DBOS_SYSTEM_DATABASE_URL=... go run .)
go run . -role=web             # the enqueue-only web tier
go run . -role=admin           # the admin view: list runs, inspect checkpoints
```

Each process prints its lines prefixed with the executor that produced them, so
across terminals you can watch work move between workers.

## What it demonstrates

| Feature | Where |
|---|---|
| `WithWorkerPool` | every `-role=worker` heartbeats a lease and sweeps for dead workers |
| Liveness takeover | a crashed worker's run resumes on a survivor (the walkthrough below) |
| `Config.ApplicationVersion` | pinned to `"v1"` so recovery is version-scoped |
| Auto executor ID | each worker gets a unique identity — no `ExecutorID` is set |
| `NewClient` + `Enqueue` | `-role=web` starts a run with no engine and polls `Client.Status` |
| `NewJob` + `RegisterJob` | `resizeJob` declares the name and both types once; the workers register it and the web tier enqueues it |
| `Client.ListRuns` + `Client.Steps` | `-role=admin` lists the fleet's newest runs (state, attempts, queue, executor, input) and dumps the newest run's checkpoints |
| `ClientConfig.ReadTimeout` | the admin client bounds every read at 10s, so a database outage fails the command instead of hanging it |
| `Client.Cancel` / `Client.Resume` | `-role=admin -cancel ID` stops a live run at its next stage; `-resume ID` revives a run no executor can still be running (retries-exceeded, or cancelled before it started) under the same ID |
| `ErrRunInFlight` | `-resume` on a run cancelled mid-stage is refused: the stage may still be running, and only `ForkFromStage` (on a worker) re-runs it safely |
| `WithStaleRunWarning` | workers warn about non-terminal runs older than 15s |
| Queue-preserving takeover | `resize-jobs` is capped at `WithConcurrency(2)`; an adopted run returns to it, so a crash cannot exceed the cap |

The cadences here are fast for the demo (1s heartbeat / 5s stale / 2s sweep); the
production defaults are 10s / 60s / 30s.

## The enqueue-only web tier

The `resize` pipeline (decode → resize → encode) is registered on the workers.
The web tier never runs it — it only enqueues and checks on it. Both sides
refer to one declaration:

```go
var resizeJob = duro.NewJob[string, string]("resize")

duro.RegisterJob(app, resizeJob, resizePipeline(id)) // worker
duro.Enqueue(c, resizeQueue, resizeJob, "vacation.jpg") // web tier
```

Because the job carries the name and both types, a rename or a type change is
a compile error in both binaries rather than a run that sits `enqueued`
forever waiting for a worker that will never recognise it.

```bash
go run . -role=worker    # terminal 1: a worker to run the job
go run . -role=web       # terminal 2: enqueue one job, poll until done
```

The web tier imports no engine and launches no queue runners; it shares only the
database. Its `duro.Status` view of the run is the exact same `RunStatus` the
workers see.

## The admin view

The admin role is the same enqueue-only `Client`, configured with
`ApplicationName: "fleet"` for reads: it registers no pipeline, launches no
engine, and sees every fleet-owned (plus migrated unclaimed) run through the
same status mapping as the workers.

```bash
go run . -role=worker            # terminal 1: a worker to run jobs
go run . -role=web               # terminal 2: enqueue a job (takes ~12s)
go run . -role=admin             # terminal 3: while it runs — and again after
```

The listing shows each run's state, attempt count, queue, executor, and (because
the example asks with `WithInput()`) its input as JSON; below it, the newest
run's checkpoints — `duro.shape` first, then one line per completed stage with
start and finish times. Run it mid-job and only `decode` is there; run it after
and `resize` and `encode` have joined it. Those checkpoints are exactly what a
takeover or a resume replays.

Remediation uses the same client. Start a job and cancel it while `resize` is
still counting:

```bash
go run . -role=admin -cancel <run-id>    # the in-flight stage finishes, the run stops at `encode`
go run . -role=admin -resume <run-id>    # refused: ErrRunInFlight
```

Watch the worker: after the cancel it logs the rest of `resize N/10` — a stage
in flight completes and checkpoints; cancellation lands at the next stage
boundary — and nothing more. The resume is refused, and that is the point: the
database says `cancelled`, but not whether the worker is still inside `resize`
(it is, for up to ten seconds, and nothing DBOS records says when it leaves).
Resuming under the same ID could run the stage twice, so `Resume` never does
it, not even after the worker has visibly moved on. The safe re-run is
`duro.ForkFromStage` on a worker: a new run that copies the checkpoints and
finishes, while this one stays cancelled.

`Resume` is for runs no executor can be running. Cancel one before it ever
starts — no worker up, so the job waits on its queue — then resume it and
start a worker:

```bash
go run . -role=web                       # terminal 1, no worker running: enqueued, waiting
go run . -role=admin -cancel <run-id>    # cancelled before any start
go run . -role=admin -resume <run-id>    # back to enqueued under the same ID
go run . -role=worker                    # terminal 2: picks it up and runs it; terminal 1 sees success
```

The same path revives a run that exceeded its recovery attempts. Nothing
no-ops silently: cancelling a finished run or resuming a successful one prints
a refusal with `duro.ErrRunTerminal`, resuming a live run prints one with
`duro.ErrRunActive` (cancel it first), and resuming a run cancelled mid-stage
prints `duro.ErrRunInFlight`.

## Crash takeover

Start a healthy worker, then start a second worker that crashes mid-run:

```bash
go run . -role=worker            # terminal 1: worker B, stays alive and sweeps
go run . -role=worker -crash     # terminal 2: worker A, starts a job then kill -9's itself
```

Worker A starts a `resize` run, logs `decode` and a few `resize N/10` steps, then
hard-exits (no graceful shutdown, so its lease is left to expire — a real crash,
not a drain). Within the stale threshold plus one sweep, worker B's sweeper
adopts the run and finishes it. Watch terminal 1: it logs the remaining
`resize N/10` steps and `encode` — but **not** `decode`, because that step's
checkpoint replays instead of re-running. Exactly-once completion; the in-flight
`resize` step is the only one that runs twice (at-least-once — keep steps
idempotent).

The adopted run comes back on `resize-jobs`, the queue it was enqueued on, not on
DBOS's internal queue — so it still counts against that queue's
`WithConcurrency(2)` cap. A run that changed queues during recovery would run
outside the limit its queue exists to enforce, and would stop counting toward it,
which is exactly when nobody is watching.

For the graceful path, stop a worker with Ctrl-C instead: it tombstones its lease
so interrupted runs are taken over on the next sweep with no stale wait.
`Close`'s timeout bounds DBOS stopping producers and unwinding local workflow
goroutines; interrupted rows remain pending. The tombstone is written after
that, so give the process a little grace beyond the timeout or a `SIGKILL`
takes the tombstone away and survivors fall back to waiting out the stale
threshold.
