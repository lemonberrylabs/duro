# CLAUDE.md

## What this is

**duro** is a Go library (`github.com/lemonberrylabs/duro`) for writing
[DBOS](https://docs.dbos.dev/) durable workflows as typed dataflow pipelines.
Every stage executes inside `dbos.RunAsStep` and checkpoints to Postgres; a
crashed process resumes mid-pipeline, replaying completed stages from their
checkpoints instead of re-running them. [samber/ro](https://github.com/samber/ro)
is the reactive engine underneath; DBOS provides the durability. DBOS is pinned
to v1.2.0 and ro remains pre-1.0 — keep dependencies to those two, plus `jackc/pgx/v5`,
used **only** where duro runs its own SQL or owns a pool: worker-pool mode
(`workerpool.go`), the `Client`'s pool (`client.go`), and the `App`'s admin pool
for `Resume` (`app.go`) — a Postgres handle DBOS does not expose (pgx is already
a transitive DBOS dep, so no new module enters the graph). Do not add a fourth.
Test-only deps OK.

## Design philosophy (non-negotiable)

- **No foot guns, no sharp edges. Assume users will not read documentation.**
  Any API that can be misused in a way that corrupts replay must either not
  compile (nominal `Stage` typing, typed channels), fail fast at
  construction/registration time (nil-fn panics, duplicate queue configs), or
  fail loudly at execution time (shape fingerprint, goroutine assertion).
  Documenting a hazard is never sufficient — the design isn't done until the
  hazard is unfireable or fails fast. Silent misbehavior is the worst outcome.
- **The public API stays closed.** `PipeN` accepts only `Stage` values; never
  expose raw `ro.Observable` composition. Escape hatches are explicit,
  named to look dangerous (`Pure`, `UnsafeOperator`).
- **Durability is the product.** New stages/operators must preserve
  deterministic step ordering under replay.

## Requirements for every change

No code lands without all of these:

1. **Tests for every addition.** Durability claims need replay proof: run,
   fork mid-pipeline (`dbos.ForkWorkflow`), assert via counters which stage
   functions re-executed. See fork-replay and shape-guard tests in
   `duro_test.go` for the pattern. Each safety guard has its own test.
2. **Tests for combinations.** A new primitive must be exercised composed
   with existing ones — inside `Sub`/`Rescue`/`Branch` arms, as a FanOut
   child, in multi-item streams — not just standalone.
3. **Docs updated.** Godoc on every exported symbol; README updated,
   including the primitives table if a stage was added or changed.
4. **Examples written.** The apps in `examples/` collectively cover the whole
   feature set — new features belong in the example whose theme fits (or a
   new example). Each example has its own README; update it (or write one)
   to document what the example demonstrates and how to run it.

## Commands

```bash
createdb duro_test           # once; tests WIPE this database every run
go test ./... -count=1       # requires a real Postgres
gofmt -l .                   # must print nothing
go vet ./...
```

- Override the test DB with `DURO_TEST_DATABASE_URL`
  (default `postgres://$USER@localhost:5432/duro_test`). Never point it at a
  database you care about.
- CI runs exactly gofmt + vet + build + test against postgres:17.
- Each example app uses its own database — see its README (e.g. `createdb
  duro_demo` for `examples/orders`).

## Architecture

Single flat package at the repo root:

- `duro.go` — `Stage`, step constructors (`Step`/`Tap`/`Filter`/`Expand`/
  `Reduce`/`Pure`/`UnsafeOperator`), step options (retries, timeout)
- `pipe.go` — `Pipe1`…`Pipe8` → `Pipeline[P, R]`, the shape fingerprint, and
  the stage-name uniqueness check (`mustPipeline`)
- `job.go` — `Job[P, R]`/`NewJob`: a pipeline's cross-process identity (name +
  both types) shared by `RegisterJob`, `Enqueue`, and `AttachJob`
- `run.go` — `Run`/`RunAll`, the hidden `duro.shape` checkpoint
- `flow.go` — control flow: `Branch`/`Switch`/`When`/`Loop`/`Rescue`/`Sub`/`Via`/`Collect`
- `fanout.go`, `parallel.go`, `queue.go` — `FanOut` + child options, `Parallel`, `NewQueue`
- `channels.go`, `signals.go` — typed `Topic`/`Event`/`Stream`; `Delay`/`Send`/
  `Recv`/`SetEvent`/`GetEvent`/`ToStream`/`FromStream`
- `app.go` — `App`, `Config` (incl. `ApplicationVersion`/`ExecutorID`),
  `New`/`Launch`/`Close`, stranded-run warning
- `register.go` — `Register`/`RegisterScheduled`/`ApplySchedules`/`RegisterDebounced`/
  `RegisterWorkflow`/`RegisterQueues`
- `handle.go`, `status.go`, `fork.go` — `Handle`, `Status`/`StatusAll`/`Attach`,
  `ForkFromStage`; `status.go` holds `runStore` (the DBOS operations the
  read/remediation APIs need, adapted from either an engine context or a
  `dbos.Client`) and the shared `listRuns`/`statusAll` mapping cores
- `list.go` — `ListRuns` + `ListOption`s (`WithNames`/`WithStates`/`WithIDs`/
  `WithApplicationNames`/`WithScheduleNames`/
  `WithCreatedAfter`/`WithCreatedBefore`/`WithQueue`/`WithLimit`/`WithOffset`/
  `WithNewestFirst`/`WithInput`), `Steps`/`StepStatus`
- `control.go` — `Cancel`, `App.Resume`, `ErrRunTerminal`/`ErrRunActive`/
  `ErrRunInFlight`; `Resume` is duro-owned SQL (`resumeSQL`, executed through
  an `execFunc` the App and Client build from their pools with `poolExec`),
  bounded by `boundContext` (client.go) like every Client read
- `workerpool.go` — worker-pool mode: `WithWorkerPool` (+ cadence options),
  `WithRetention`, `WithStaleRunWarning`; the heartbeat lease, sweeper +
  liveness takeover, retention, all on a dedicated pgx pool
- `client.go` — enqueue-only `Client`: `NewClient`, generic `Enqueue`, and the
  full read/remediation surface (`Status`/`StatusAll`/`ListRuns`/`Steps`/
  `Cancel`/`Resume`) as thin calls into the same `runStore` cores the engine
  uses. Enqueue goes through `dbos.Client`; every read runs on a duro-owned,
  never-launched DBOS context (`reads`) derived per call with `readContext`,
  bounded by `ClientConfig.ReadTimeout` and, on a `WithContext` view, the
  caller's context

## Gotchas

- **Replay determinism is the core invariant**: on recovery, the Nth step call
  must be the same logical operation as in the original run. Concurrency,
  timers, and non-deterministic construction all violate it — that's what the
  three guard layers (compile / construction / execution) exist to catch.
- `Pure` functions and pipeline construction must be deterministic; the
  library cannot guard this.
- A registered pipeline's name is its **durable identity** — renaming strands
  in-flight runs (`app.Launch()` warns about them). Same name must be
  registered on every process start.
- Embedded pipelines (`Branch`/`Switch`/`Loop`/`Rescue`/`Sub`/`Via` arms)
  fold into the shape fingerprint — editing an arm changes the fingerprint.
- **The shape fingerprint's exact text is on-disk format.** `stageKind` values,
  the `fingerprint*Sep` separators, and the `branchThenKey`/`branchElseKey`
  route keys all render into the checkpoint compared on every replay: change
  one byte and every in-flight run built by the previous binary fails its shape
  check forever. `shape_internal_test.go` holds golden fingerprints captured
  from the released code — a diff there is a durability break, never a stale
  expectation to update.
- `Run` returns exactly one value: zero emissions is `ErrNoValue`, more than one
  is `ErrMultipleValues`. Never "fix" that by picking a value — silently
  dropping results is the foot gun it exists to prevent. Use `RunAll`, or fold
  with `Reduce`/`Collect`.
- Options that only some stages honor (`WithCancelSiblingSteps` → `Parallel`,
  `WithMaxIterations` → `Loop`) must panic at construction on every other
  constructor; see `resolveCancelSiblings`/`resolveLoopBound`. A new stage
  constructor that accepts `StepOption` has to call both, or it silently
  swallows an option the caller believes is in effect.
- Worker-pool takeover (`workerpool.go`) issues its own SQL `UPDATE` against
  DBOS's internal `dbos.workflow_status` table — a deliberate coupling to DBOS's
  schema, pinned to the dbos module version. It is the only safe re-enqueue
  (public `ResumeWorkflows` has a double-execution yank race and misses
  NULL-`started_at` direct runs). It fails **loud** on schema drift (a renamed
  column is an SQL error, surfaced in the sweep log and the R2b integration
  tests) — re-verify it when bumping dbos. Three non-obvious invariants inside
  it: it must **not** reset `recovery_attempts` (that is DBOS's poison-run
  circuit breaker; zeroing it lets a process-killing run walk the fleet forever);
  queued runs are only adoptable because DBOS stamps `executor_id` and
  `application_version` at dequeue (pinned by `TestTakeoverClientEnqueuedRun`);
  and `queue_name` follows DBOS's *recovery*, not its *resume* — an adopted run
  goes back on its own queue (`COALESCE(NULLIF(queue_name, ''), internal)`) or it
  escapes that queue's concurrency and rate limits. The `NULLIF` is load-bearing:
  DBOS stores `''`, not NULL, for a directly started run, so a plain `COALESCE`
  strands those runs `ENQUEUED` on a queue nobody polls. Both halves are pinned
  by `TestTakeoverPreservesQueue` and `TestTakeoverReturnsRunToItsQueue`.
- Worker-pool housekeeping must never share the sweeper's advisory lock or run
  unbounded work: takeover is the safety-critical path, and the maintenance
  goroutine drives both. Retention deletes one batch per cycle under its own
  lock, and every background query — duro's own and the DBOS-routed ones — is
  deadline-bounded.
- Every maintenance database call must hang off the maintenance scope: pgx ones
  off `maintCtx`, DBOS-routed ones off `maintDctx` (`initMaintContexts`). One
  built from the app's DBOS context compiles and works, but ignores
  `stopMaintenance` — so `Close` blocks on `maintWG` for a full `opTimeout`
  before DBOS shutdown starts, usually costing the tombstone. Pinned by
  `TestMaintenanceDBOSCallsObserveStopMaintenance`.
- An **absent** heartbeat row means "unknown executor, do not touch its runs";
  a **tombstoned** one (epoch timestamp) means "known-dead, adopt now". Lease
  pruning therefore must skip executors that still own non-terminal runs, or
  those runs become permanently unadoptable.
- Every run read or remediation — engine function and `Client` method alike —
  must be a thin call into the shared `runStore` core (`status.go`), never a
  parallel implementation: an api tier on a `Client` and the workers must agree
  on every State, on "terminal", and on which runs `Resume` accepts. `Resume`
  deliberately refuses live runs (`ErrRunActive` — DBOS's resume re-enqueues a
  PENDING run while its executor may still be running it, the double-execution
  yank) and success/error runs (`ErrRunTerminal` — DBOS silently no-ops those);
  `Cancel` refuses terminal runs for the same reason. An empty membership
  filter (`WithIDs()`) matches nothing, never everything.
- **`Resume`'s state guard is the `UPDATE`'s own `WHERE`** (`resumeSQL`:
  `status = MAX_RECOVERY_ATTEMPTS_EXCEEDED OR (status = CANCELLED AND
  recovery_attempts = 0)`), never a check-then-act on `dbos.ResumeWorkflow`:
  DBOS's guard admits PENDING, so two callers that both saw "cancelled" would
  have the second re-enqueue a run the first one's worker is already
  executing. The `recovery_attempts = 0` half is the quiescence rule:
  CANCELLED does **not** mean the executor stopped — cancellation lands at the
  next step start, the in-flight stage runs to completion, and nothing in the
  schema records the goroutine's exit — so a run cancelled after any start is
  refused forever (`ErrRunInFlight`; the remedy is `ForkFromStage`). Do not
  "fix" that by waiting or guessing. Like takeover it is coupled to DBOS's
  schema; unlike takeover it resets `recovery_attempts` on purpose (an
  operator's explicit action grants a fresh budget, as DBOS's resume does) and
  keeps the run's queue via `COALESCE(NULLIF(queue_name, ''), internal)`.
  Pinned by `TestResumeTransitionGuard` and
  `TestResumeRefusesRunCancelledMidExecution`.
- **Never rely on DBOS's `loadInput`/`loadOutput` defaults.** They are
  "on once `Launch()` ran" — true on an engine, never on a `dbos.Client`
  (`NewClient` never launches). A query that omits `WithLoadOutput(true)`
  reads the recorded error on the workers and the placeholder
  `duro: run error` in an api tier, and nothing local reproduces it because
  dev runs the engine. Pass both flags explicitly on every `ListWorkflows`
  call; pinned by `TestClientFailedRunExposesError`.
- **Client reads must go through `readContext`, once per public call**, never
  `c.c.ListWorkflows` and friends on the root `dbos.Client`: although the v1
  interface embeds `context.Context`, it does not expose the DBOS context-
  derivation methods needed to retain client capabilities with a tighter
  deadline. DBOS retries every failed read forever (`maxRetries: -1`, backoff
  to 30s) until its context ends — a read on the root hangs an api-tier
  goroutine for the length of a database outage. One context per call, not
  per query, or a two-query call takes two timeouts. `readContext` takes the
  sooner of `ReadTimeout` and the caller's deadline and only propagates the
  caller's *cancellation*, so a deadline surfaces as `DeadlineExceeded`, not
  `Canceled`. `Enqueue` and `Handle` waits still hang; documented, not fixable
  from duro. Pinned by `TestClientReadTimeoutBoundsBlockedRead` and
  `TestClientWithContextDeadlineIsDeadline`.
- `go build` in `examples/` drops binaries (e.g. `housekeeping`, `fleet`) —
  don't commit them.
- Open an issue before behavior changes or new primitives (per CONTRIBUTING.md).
