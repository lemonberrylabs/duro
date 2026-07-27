# fleet — worker-pool mode

The infrastructure side of duro: a fleet of interchangeable workers that recover
each other's runs on ephemeral hardware (no DBOS Conductor), and an enqueue-only
web tier that starts work without running an engine.

```bash
createdb duro_fleet
go run . -role=worker          # a worker  (or: DBOS_SYSTEM_DATABASE_URL=... go run .)
go run . -role=web             # the enqueue-only web tier
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
| `WithStaleRunWarning` | workers warn about non-terminal runs older than 15s |

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

For the graceful path, stop a worker with Ctrl-C instead: it tombstones its lease
so any run it could not drain is taken over on the next sweep with no stale wait.
