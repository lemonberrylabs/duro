# CLAUDE.md

## What this is

**duro** is a Go library (`github.com/lemonberrylabs/duro`) for writing
[DBOS](https://docs.dbos.dev/) durable workflows as typed dataflow pipelines.
Every stage executes inside `dbos.RunAsStep` and checkpoints to Postgres; a
crashed process resumes mid-pipeline, replaying completed stages from their
checkpoints instead of re-running them. [samber/ro](https://github.com/samber/ro)
is the reactive engine underneath; DBOS provides the durability. Both deps are
pre-1.0 and pinned — keep dependencies to exactly those two (test-only deps OK).

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
- `pipe.go` — `Pipe1`…`Pipe8` → `Pipeline[P, R]`
- `run.go` — `Run`/`RunAll`, the hidden `duro.shape` checkpoint
- `flow.go` — control flow: `Branch`/`Switch`/`When`/`Loop`/`Rescue`/`Sub`/`Collect`
- `fanout.go`, `parallel.go`, `queue.go` — `FanOut` + child options, `Parallel`, `NewQueue`
- `channels.go`, `signals.go` — typed `Topic`/`Event`/`Stream`; `Delay`/`Send`/
  `Recv`/`SetEvent`/`GetEvent`/`ToStream`/`FromStream`
- `app.go` — `App`, `Config`, `New`/`Launch`/`Shutdown`, stranded-run warning
- `register.go` — `Register`/`RegisterScheduled`/`RegisterDebounced`/
  `RegisterWorkflow`/`RegisterQueues`
- `handle.go`, `status.go`, `fork.go` — `Handle`, `Status`/`StatusAll`/`Attach`,
  `ForkFromStage`

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
- Embedded pipelines (`Branch`/`Switch`/`Loop`/`Rescue`/`Sub` arms) fold into
  the shape fingerprint — editing an arm changes the fingerprint.
- `go build` in `examples/` drops binaries (e.g. `housekeeping`) — don't
  commit them.
- Open an issue before behavior changes or new primitives (per CONTRIBUTING.md).
