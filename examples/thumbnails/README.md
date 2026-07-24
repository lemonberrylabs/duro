# thumbnails — fan-out fleets, queues, and child options

A gallery renderer that fans a batch of images out **twice** — renders on a
bounded, priority-scheduled queue, deliveries on a partitioned queue — and
folds a manifest, then runs a **strict** batch whose first failure cancels
the surviving fleet. Every knob duro exposes for child workflows appears
here.

```bash
createdb duro_thumbnails
go run .    # or: DBOS_SYSTEM_DATABASE_URL=... go run .
```

## What it demonstrates

| Feature | Where |
|---|---|
| `NewQueue` + `WithConcurrency`, `WithRateLimit`, `WithPriorities` | `renderQueue` — 3 concurrent renders fleet-wide, ≤20 starts/s |
| `WithPartitions`, `WithWorkerConcurrency` | `deliverQueue` — per-region concurrency, 2 deliveries per executor |
| Automatic queue registration | no `RegisterQueues` call anywhere: `Register` sees the queues the pipelines reference |
| `FanOut` + `*RegisteredWorkflow` | the render stage fans out into the hand-written workflow's ref |
| `FanOut` + pipeline child | the deliver stage fans out into `DeliverPipeline` — a `*PipelineWorkflow` passed directly |
| `WithChildID` | renders are idempotent across gallery runs: the rerun re-attaches instead of re-rendering |
| `WithChildDeduplicationID` + `duro.DeduplicationReturnExisting` | the duplicate upload collapses onto its sibling's render |
| `WithChildPriority`, `WithChildTimeout`, `WithChildAuthenticatedUser` | render children: scheduling + durable deadline + auth metadata |
| `WithChildPartitionKey`, `WithChildDelay` | delivery children: routed to per-region partitions, started DELAYED |
| `duro.RegisterWorkflow` + `duro.Context` + `RunAll` | `RenderImage` — hand-written, registered, and fanned out with zero dbos imports |
| `Parallel` | the three sizes render concurrently in-process, bounded to 2, one checkpointed step each |
| `WithCancelSiblings` + `Rescue` | the strict gallery: a corrupt image cancels the slow sibling renders mid-flight; the refund handler sees the corrupt image's error |
| `duro.Status` | the strict children's terminal states, read back after the batch settles |

## Reading the output

**Run 1** renders 3 of 4 images: `img-3` carries the same content hash as
`img-1`, so its enqueue returns the existing render (`DeduplicationReturnExisting`)
— and its manifest entry shows `img-1`'s result. That is what deduplication
*means*: both items observe the first child's output.

**Run 2** executes exactly 1 render: `img-1/2/4` re-attach to their finished
children by ID (`WithChildID` makes child identity application-level, so a
re-run of the whole gallery is nearly free), while `img-3` — which never had
a child of its own — renders now that no duplicate is active. Deduplication
is a *concurrency* collapse; idempotent IDs are a *history* collapse. The two
compose, and this run shows the seam between them.

**Run 3** is the strict gallery: two slow renders (3s "model calls") plus a
corrupt image that fails immediately. With `WithCancelSiblings` on the
fan-out, the corrupt image's failure cancels both slow siblings mid-render —
the batch settles in about a second instead of draining for 3+ seconds — and
the `Rescue` around the fleet receives the *corrupt image's* error as its
cause (never a sibling's CANCELLED result), issues the refund, and the
pipeline completes with an empty manifest. The child states printed at the
end show the split: the corrupt image `error`, its siblings `cancelled`.

Drain vs cancel is a per-stage choice, not an application-wide one: the
delivery fan-out in runs 1–2 keeps the drain default because a delivery that
completes is valuable regardless of its siblings, while the strict render
fleet cancels because renders for a failed batch are money spent on an
outcome already decided.

## The hand-written workflow

`RenderImage` is a plain workflow function — `func(ctx duro.Context, img
Image) (Rendered, error)` — because sometimes you want imperative code around
a pipeline (`RunAll` here returns every emitted value for post-processing).
It registers like everything else, and the result is a `WorkflowRef` that
`FanOut` accepts directly:

```go
render := duro.RegisterWorkflow(app, "RenderImage", RenderImage)
...
duro.FanOut("render", renderQueue, render, ...)
```

Prefer registered pipelines (`duro.Register`) unless you need imperative
control flow the pipeline shape cannot express — branching between
pipelines, loops, or reshaping a `RunAll` result.

## Things to try

- Remove `WithCancelSiblings` from the strict fan-out and re-run: the refund
  still fires with the same cause, but only after every slow render finishes —
  the settle time is the cost of drain semantics.
- Bump `WithConcurrency(3)` down to 1 and watch deliveries serialize.
- Give the batch 20 images and watch the rate limiter pace render starts.
- The demo scopes run and child IDs to the invocation (`batchTag` in
  `main.go`) so it stays repeatable against a used database. Delete the tag —
  production-style stable IDs — then kill the process mid-gallery and run
  again: the parent and every child it already enqueued are re-attached, not
  re-run.
