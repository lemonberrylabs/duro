# thumbnails — fan-out fleets, queues, and child options

A gallery renderer that fans a batch of images out **twice** — renders on a
bounded, priority-scheduled queue, deliveries on a partitioned queue — and
folds a manifest. Every knob duro exposes for child workflows appears here.

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
| `FanOut` + `duro.Workflow(fn)` | the render stage fans out into a hand-written workflow |
| `FanOut` + pipeline child | the deliver stage fans out into `DeliverPipeline` — a `*PipelineWorkflow` passed directly |
| `WithChildID` | renders are idempotent across gallery runs: the rerun re-attaches instead of re-rendering |
| `WithChildDeduplicationID` + `duro.DeduplicationReturnExisting` | the duplicate upload collapses onto its sibling's render |
| `WithChildPriority`, `WithChildTimeout`, `WithChildAuthenticatedUser` | render children: scheduling + durable deadline + auth metadata |
| `WithChildPartitionKey`, `WithChildDelay` | delivery children: routed to per-region partitions, started DELAYED |
| `duro.Context` + `RunAll` | `RenderImage` — a hand-written workflow whose body is dbos-free |
| `Parallel` | the three sizes render concurrently in-process, bounded to 2, one checkpointed step each |

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

## The hand-written boundary

`RenderImage` is a plain workflow function — `func(ctx duro.Context, img
Image) (Rendered, error)` — because sometimes you want imperative code around
a pipeline. Its body stays dbos-free (`duro.Context` is an alias for DBOS's
context; `RunAll` returns every emitted value). Registration is the one place
the dbos package appears, in `main.go`:

```go
dbos.RegisterWorkflow(app.Context(), RenderImage, dbos.WithWorkflowName("RenderImage"))
```

Registered pipelines (`duro.Register`) never need this — prefer them unless
you need imperative control flow around the pipeline.

## Things to try

- Bump `WithConcurrency(3)` down to 1 and watch deliveries serialize.
- Give the batch 20 images and watch the rate limiter pace render starts.
- Kill the process mid-gallery and run again with the same run ID: the parent
  re-attaches to every child it already enqueued.
