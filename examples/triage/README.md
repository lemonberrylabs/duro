# triage — durable control flow as pipeline stages

A support-ticket triage system whose branching, looping, and reuse all live
*inside* one registered pipeline — the control flow that used to require a
hand-written workflow function, expressed as typed stages.

```bash
createdb duro_triage
go run .    # or: DBOS_SYSTEM_DATABASE_URL=... go run .
```

## What it demonstrates

| Feature | Where |
|---|---|
| `Switch` + `When` | tickets dispatch by category to the billing / bug / spam arm |
| `Branch` (nested inside a Switch arm) | urgent bugs escalate; routine bugs get filed — control flow composes |
| `Loop` | the escalation path durably polls the fix service (with a `Delay` backoff in the body) until it reports fixed |
| `Rescue` (best-effort segment) | the billing arm's loyalty credit retries (`WithMaxRetries`), then the handler swallows the terminal failure — the refund must not die for a goodwill credit |
| `Rescue` (whole-pipeline except block) | `GuardedTriagePipeline` wraps the entire triage: report the failure, then rethrow it |
| `Sub` | one shared `notify` segment reused by three arms under different names |
| `Collect` | the batch folds into a report of every resolution, in order |

## How this stays replay-deterministic

Every routing decision is a **checkpointed step** — the same mechanism
`Filter` has always used. `Switch`'s route key, `Branch`'s verdict, each
`Loop` iteration's continue/stop decision, and `Rescue`'s handler outcome are
recorded; on recovery the workflow re-reads them and walks exactly the same
stage sequence. A recovered run can never flip a swallowed failure into a
propagated one: failed steps replay their recorded error, and the rescue
decision replays from the handler's checkpoint. The demo's second section
proves it: re-running the completed workflow ID replays the entire triage —
including all three poll iterations and the exhausted loyalty retries — with
**zero** re-executions.

The embedded pipelines are part of the pipeline's *shape*: edit a Branch arm
or Loop body between deploys and a replaying run fails fast with a
shape-mismatch error instead of misreading checkpoints.

## When you still want a hand-written workflow

These combinators cover routing, iteration, composition, and error handling.
What's left for `duro.RegisterWorkflow` + imperative code is genuinely
irreducible glue: interop with existing DBOS codebases, or logic that doesn't
fit a dataflow shape at all.

## Things to try

- Kill the process while the urgent bug is mid-`Loop` (between polls), run
  again: the loop resumes at the recorded iteration, not from zero.
- Kill the process between the loyalty credit's terminal failure and the
  `Rescue` handler's checkpoint: recovery replays the recorded failure
  (without re-running the retries) and re-reaches the handler.
- Add a case to the `Switch` and re-run: new runs use it; a fork of an old
  run trips the shape guard — the nested shapes are part of the fingerprint.
- Route a ticket with an unknown category through the unguarded pipeline: it
  fails loudly with `no case for route key`. The guarded pipeline reports
  the same failure first — that's the difference `Rescue` makes.
