<h1 align="center">duro</h1>

<p align="center"><em>Durable dataflow pipelines for Go.</em></p>

<p align="center">
  <a href="https://github.com/lemonberrylabs/duro/actions/workflows/ci.yml"><img src="https://github.com/lemonberrylabs/duro/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/lemonberrylabs/duro"><img src="https://pkg.go.dev/badge/github.com/lemonberrylabs/duro.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/lemonberrylabs/duro"><img src="https://goreportcard.com/badge/github.com/lemonberrylabs/duro" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

**duro** lets you write [DBOS](https://docs.dbos.dev/) durable workflows as typed
dataflow pipelines. Every stage is checkpointed to Postgres: if your process
dies mid-pipeline, the workflow resumes exactly where it left off — completed
stages replay from their checkpoints instead of re-running. And the API is
designed so that code which would corrupt recovery either doesn't compile or
fails fast with a clear error.

```go
var OrderPipeline = duro.Pipe4(
	duro.Step("validate", validateOrder),
	duro.Step("reserve", reserveInventory),
	duro.Step("charge", chargePayment, duro.WithMaxRetries(3)),
	duro.Step("notify", sendConfirmation),
)

// at startup:
orders := duro.Register(app, "orders", OrderPipeline)
// anywhere:
handle, err := orders.Start(app, order)
confirmation, err := handle.Result()
```

Kill the process right after `reserve` completes. On restart, `validate` and
`reserve` replay from Postgres in microseconds — without executing your
functions — and the workflow continues at `charge`. Durable orchestration
without writing a single line of recovery code.

## Why duro

- **Durable by construction** — each stage runs inside `dbos.RunAsStep`; DBOS
  checkpoints the result and recovers crashed workflows automatically.
- **Typed end to end** — `Pipe4(Step[Order→Validated], Step[Validated→Reservation], …)`
  type-checks the whole chain; mismatched stages don't compile.
- **Streams, not just sequences** — `Expand`, `Filter`, and `Reduce` process
  collections item by item, with a checkpoint per stage execution.
- **Safety as a feature** — three guard layers (compiler, construction-time,
  execution-time) make the classic durable-workflow footguns hard to fire; see
  [Built-in safety](#built-in-safety).
- **The whole DBOS toolkit, typed** — messaging, events, and streams (both
  directions), scheduled and debounced pipelines, stage-level forking, and
  per-child queue controls — each as a small, composable surface over the
  DBOS primitive it wraps.

## Installation

```bash
go get github.com/lemonberrylabs/duro
```

Requires Go 1.26+ and PostgreSQL (DBOS's system database).

## Quickstart

A complete durable application — note the single import:

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lemonberrylabs/duro"
)

type Order struct {
	ID          string
	AmountCents int
}

type Receipt struct {
	OrderID   string
	PaymentID string
}

var ChargePipeline = duro.Pipe2(
	duro.Step("charge", func(_ context.Context, o Order) (Receipt, error) {
		return Receipt{OrderID: o.ID, PaymentID: "pay-" + o.ID}, nil // call your payment provider here
	}, duro.WithMaxRetries(3)),
	duro.Tap("audit", func(_ context.Context, r Receipt) error {
		fmt.Printf("audited %s\n", r.OrderID)
		return nil
	}),
)

func main() {
	app, err := duro.New(context.Background(), duro.Config{
		Name:        "quickstart",
		DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
	})
	if err != nil {
		panic(err)
	}

	charge := duro.Register(app, "charge-order", ChargePipeline)

	if err := app.Launch(); err != nil {
		panic(err)
	}
	defer app.Shutdown(5 * time.Second)

	handle, err := charge.Start(app, Order{ID: "42", AmountCents: 1999})
	if err != nil {
		panic(err)
	}
	receipt, err := handle.Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("charged: %+v\n", receipt)
}
```

```bash
createdb quickstart
DBOS_SYSTEM_DATABASE_URL=postgres://$USER@localhost:5432/quickstart go run .
```

## The primitives

| Stage | What it does | Durable? |
|---|---|---|
| `duro.Step(name, fn)` | transform `T → R` | ✅ checkpointed step |
| `duro.Tap(name, fn)` | side effect, passes `T` through | ✅ checkpointed step |
| `duro.Filter(name, pred)` | drop items failing the predicate | ✅ predicate is a step |
| `duro.Expand(name, fn)` | one item → many (`T → []R`), emitted in order | ✅ checkpointed step |
| `duro.Reduce(name, fn, seed)` | fold the stream; emits the final accumulator | ✅ one step per accumulation |
| `duro.FanOut(name, queue, wf)` | parallel map: each item runs as a child workflow on a declared `Queue` | ✅ every child is a durable workflow |
| `duro.Parallel(name, max, fn)` | parallel map: concurrent steps in-process, at most `max` at a time | ✅ pre-assigned step per item |
| `duro.Delay(name, d)` | durable pause per item | ✅ recovery resumes the remaining time |
| `duro.Send(name, topic, fn)` | message another workflow's mailbox on a typed `Topic` | ✅ no re-send on replay |
| `duro.Recv(name, topic, timeout)` | pause until a `Topic` message arrives; emits it | ✅ no double-consume on replay |
| `duro.SetEvent(name, event, fn)` | publish per-item progress on a typed `Event` | ✅ checkpointed |
| `duro.GetEvent(name, event, fn, timeout)` | read another workflow's `Event`; emits it | ✅ replay returns the observed value |
| `duro.ToStream(name, stream)` | append items to a typed durable `Stream`, closed at completion | ✅ checkpointed |
| `duro.FromStream(name, stream, fn)` | drain another workflow's `Stream`; emits its values | ✅ the whole read is one checkpoint |
| `duro.Pure(name, fn)` | cheap reshaping between stages | ❌ re-executes on replay — must be deterministic and side-effect free |
| `duro.UnsafeOperator(name, op)` | escape hatch for raw ro operators | ⚠️ you're on your own (runtime guards still apply) |

Stages compose with `duro.Pipe1`…`Pipe8` into a `Pipeline[P, R]`. Register
one as a workflow (`duro.Register`, below) or run it inside a hand-written
workflow body:

- `duro.Run(ctx, input, pipeline)` — returns the last emitted value
- `duro.RunAll(ctx, input, pipeline)` — returns every emitted value

Hand-written workflows are declared against `duro.Context` — an alias for
DBOS's context type, so it works with every dbos API while workflow code
imports only duro:

```go
func InvoiceWorkflow(ctx duro.Context, b Batch) (Invoice, error) {
	return duro.Run(ctx, b, InvoicePipeline)
}
```

### Stage options

Every step-backed stage takes functional options:

- **Retries** — `WithMaxRetries(n)`, `WithBaseInterval(d)`, `WithMaxInterval(d)`,
  `WithBackoffFactor(f)` for the exponential-backoff envelope, and
  `WithRetryPredicate(pred)` to spend retries on transient errors only:

  ```go
  duro.Step("charge", chargePayment,
      duro.WithMaxRetries(5),
      duro.WithBaseInterval(200*time.Millisecond),
      duro.WithMaxInterval(10*time.Second),
      duro.WithRetryPredicate(func(err error) bool { return !errors.Is(err, ErrCardDeclined) }),
  )
  ```

- **Timeouts** — `WithTimeout(d)` cancels each attempt's step context after
  `d` (per attempt; the stage function must honor cancellation). For a durable
  deadline on a whole pipeline run, start its workflow from a context derived
  with `dbos.WithTimeout`; for child workflows, see `WithChildTimeout` below.

- **Serialization** — declare a channel with `duro.Portable()`
  (`duro.NewTopic[M](name, duro.Portable())`) to write its payloads in DBOS's
  cross-language format so Python/TypeScript DBOS apps can consume them.
  Readers need nothing special — DBOS decodes by each value's recorded
  serialization.

### Multi-item pipelines

Durability per item, not just per workflow — each stage execution below is its
own checkpoint, so a crash mid-batch resumes at the exact item and stage where
it stopped:

```go
func InvoiceWorkflow(ctx duro.Context, b Batch) (Invoice, error) {
	return duro.Run(ctx, b, duro.Pipe5(
		duro.Expand("explode", func(_ context.Context, b Batch) ([]LineItem, error) {
			return b.Items, nil
		}),
		duro.Filter("in-stock", func(_ context.Context, li LineItem) (bool, error) {
			return li.InStock, nil
		}),
		duro.Step("price", priceItem),
		duro.Tap("audit", auditItem),
		duro.Reduce("total", func(_ context.Context, acc Invoice, p PricedItem) (Invoice, error) {
			acc.TotalCents += p.TotalCents
			acc.ItemCount++
			return acc, nil
		}, Invoice{BatchID: b.ID}),
	))
}
```

### Parallel fan-out

Need "20 jobs, at most 4 at a time, merge the results"? `FanOut` runs each
item as a **child workflow** on a DBOS queue, so parallelism, rate limits, and
distribution across processes are all governed by the queue — and every child
is independently durable:

```go
var Jobs = duro.NewQueue("jobs", duro.WithConcurrency(4)) // declared once, referenced by value

var ProcessAll = duro.Pipe3(
	duro.Expand("explode", func(_ context.Context, js []Job) ([]Job, error) { return js, nil }),
	duro.FanOut("process", Jobs, duro.Workflow(ProcessJob)), // ProcessJob: a hand-written workflow
	duro.Reduce("merge", mergeResults, Merged{}),
)
```

Registering a pipeline with `Register` automatically registers every queue it
references — no separate registration call, no name to keep in sync. (For
pipelines run with `Run` inside hand-written workflows, call
`duro.RegisterQueues(app, Jobs)` once at startup.) Queue knobs are duro
options: `WithConcurrency`, `WithWorkerConcurrency`, `WithRateLimit`,
`WithPriorities`, `WithPartitions`. Declaring the same queue name twice with
different configurations fails loudly at registration.

Children are enqueued in stream order with IDs derived from the parent's step
counter, so a recovered parent re-attaches to its children instead of spawning
duplicates; results are awaited and emitted in input order, each checkpointed
in the parent. Determinism is preserved because DBOS checkpoints both the
spawns and the awaits.

Child workflows are configured with `ChildOption`s. Identity options derive a
per-child value from the item; policy options apply to every child:

```go
duro.FanOut("process", Jobs, duro.Workflow(ProcessJob),
	duro.WithChildID(func(j Job) string { return "job-" + j.ID }), // idempotency across runs
	duro.WithChildDeduplicationID(func(j Job) string { return j.CustomerID }),
	duro.WithChildDeduplicationPolicy(duro.DeduplicationReturnExisting),
	duro.WithChildPartitionKey(func(j Job) string { return j.Region }),
	duro.WithChildPriority(2),                    // queue must enable priorities
	duro.WithChildDelay(time.Minute),             // start children DELAYED
	duro.WithChildTimeout(10*time.Minute),        // durable per-child deadline
	duro.WithChildAuthenticatedUser("billing-svc"),
	duro.WithChildAppVersion(version),            // pin recovery to a code version
	duro.WithPortableChildren(),                  // cross-language serialization
)
```

Item-derived options are typed by the stage's item; a mismatch panics at
construction time, like every other stage validation. A registered pipeline is
itself a valid FanOut child — pass it directly:
`duro.FanOut("sub", Jobs, childPipeline)`.

When you don't need queue-level distribution or per-child durability,
`duro.Parallel(name, max, fn)` is the lightweight sibling: concurrent **steps**
inside the workflow process (built on `dbos.Go`, which pre-assigns each step's
ID deterministically), bounded by `max`, with results in input order — no
queue, no polling latency.

### Signals, events, and streams

Messaging goes through **typed channels**: declare a `Topic`, `Event`, or
`Stream` once, and both sides reference the value. The key lives in one place
and the payload type is compiler-checked — sending an `Approval` where the
receiver expects an `Invoice` doesn't compile:

```go
var (
	Approvals = duro.NewTopic[Approval]("approvals")
	Receipts  = duro.NewStream[Receipt]("receipts")
)

duro.Pipe5(
	duro.Step("prepare", prepare),
	duro.Delay[Prepared]("cool-off", 24*time.Hour),        // durable sleep — survives restarts
	duro.Recv[Prepared]("await-approval", Approvals, 72*time.Hour),
	duro.Step("execute", execute),                          // human-in-the-loop, durably
	duro.ToStream("publish", Receipts),                     // readers consume incrementally
)
```

- `Delay` checkpoints its wake-up deadline: a workflow recovered mid-sleep
  sleeps only the remaining time; a replayed sleep is instant.
- `Recv` parks the pipeline until a message arrives on the workflow's mailbox;
  receipt is checkpointed so recovery never consumes a second message. `Send`
  is the in-pipeline counterpart; from a plain client, `Approvals.Send(app,
  workflowID, approval)`.
- `SetEvent` publishes per-item progress readable while the pipeline runs;
  `ToStream` appends each item to a durable stream, closed when the pipeline
  completes. Clients read with `event.Get(app, id, timeout)` and
  `stream.Read(app, id)`.
- Both have read-side stages: `GetEvent` durably observes another workflow's
  event, and `FromStream` drains another workflow's stream into the pipeline —
  one checkpoint for the whole read, so replay never re-reads a stream that
  has since changed. Bound the wait with `duro.WithTimeout`.

### Pipelines as workflows

`Register` makes a pipeline a first-class DBOS workflow — no wrapper function
to write — and two variants cover scheduling and burst-collapsing:

```go
wf := duro.Register(app, "invoice", invoicePipeline) // after duro.New, before app.Launch
handle, err := wf.Start(app, batch)                  // → duro.Handle[Invoice]
result, err := handle.Result()

// Cron pipelines: input is the tick time, enforced at compile time.
duro.RegisterScheduled(app, "nightly-report", "0 0 2 * * *",
	reportPipeline) // Pipeline[time.Time, Report]

// Debounced pipelines: bursts collapse into one run with the last input.
deb := duro.RegisterDebounced(app, "reindex", reindexPipeline)
deb.Debounce(app, userID, 30*time.Second, req) // each call pushes the start back
```

The registered name is the pipeline's **durable identity**: in-flight runs are
recovered by looking it up, so register the same name on every process start.
`app.Launch()` checks for runs recorded under names that are no longer
registered and warns about each one — a renamed pipeline is a startup warning,
not a silent recovery failure.

### Forking from a stage

`ForkFromStage` restarts an existing run from a named stage: earlier stages
replay from checkpoints, the named stage and everything after re-execute —
optionally on a different application version (the recovery tool for re-running
a workflow on fixed code after a bad deploy):

```go
handle, err := duro.ForkFromStage[Confirmation](app, duro.Fork{
	WorkflowID:         failedRunID,
	Stage:              "charge",
	ApplicationVersion: fixedVersion, // optional; inherits when empty
})
```

## Built-in safety

DBOS replay determinism requires that the Nth step call on recovery is the
same logical operation as in the original run. Reactive operators make that
easy to violate — concurrency reorders steps between runs, timers change
emission counts — and the worst failure is *silent*: two identically-named
steps swapping positions hand one step the other's checkpointed output. duro
guards this in three layers:

1. **Compile time.** `PipeN` only accepts `Stage` values, which only duro's
   constructors can create (nominal struct typing). Raw ro operators don't
   type-check, and `Run` owns the source — there is no way to plug in a
   channel-fed or timer-based stream.
2. **Construction time.** `Run` checkpoints the pipeline's shape (ordered
   stage kinds + names) as a hidden first step, `duro.shape`. A replay that
   builds a different pipeline — non-deterministic construction, changed
   code — fails immediately with a shape-mismatch error instead of reading
   misaligned checkpoints.
3. **Execution time.** Every stage asserts it runs on the goroutine the
   pipeline was subscribed on, catching smuggled concurrency before it can
   race DBOS's step counter. After any stage failure, a shared abort flag
   stops items behind the failure from executing further stages — fail-fast,
   like sequential code.

What remains yours, as in every durable-workflow system: `Pure` functions and
pipeline construction must be deterministic.

## How it works

`duro.Context` (DBOS's context type) implements `context.Context`, and
[samber/ro](https://github.com/samber/ro) — the reactive engine under duro's
hood — propagates the subscription context unchanged through its operators.
`duro.Run` subscribes the composed pipeline with the workflow's context; each
stage recovers it and executes its function via `dbos.RunAsStep`, one durable
checkpoint per stage execution. Synchronous emission on the workflow goroutine
keeps step order deterministic — exactly what DBOS replay requires.

## Example apps

Each example is a runnable app with its own README; together they cover the
whole feature set:

- [`examples/payments`](examples/payments) — **signals & resilience**: retry
  options and predicates, per-attempt timeouts, durable pauses,
  human-in-the-loop approval over a typed Topic, progress Events, and a
  portable receipt Stream drained by a second pipeline.
- [`examples/thumbnails`](examples/thumbnails) — **fan-out fleets**: queue
  declarations (concurrency, rate limits, priorities, partitions), every
  child option (idempotent IDs, deduplication, timeouts, delays, auth), a
  hand-written child on `duro.Context` with `RunAll` and `Parallel`, and a
  registered pipeline used directly as a FanOut child.
- [`examples/housekeeping`](examples/housekeeping) — **operations**: cron
  pipelines, debounced bursts, forking a finished run from a named stage
  after a fix, and a three-phase walkthrough of the durable-identity
  contract and the stranded-run warning.
- [`examples/orders`](examples/orders) — **the fundamentals**: the same order
  workflow written both as plain sequential DBOS steps and as a duro pipeline
  (their recorded checkpoints are identical), plus a crash-recovery demo:

  ```bash
  createdb duro_demo
  cd examples/orders
  go run .                                      # all variants + step-sequence dumps
  go run . -variant=duro -crash-after=reserve   # die mid-workflow…
  go run .                                      # …recover: watch replayed steps keep their old timestamps
  ```

## Status

Experimental. The durability semantics are covered by a test suite that runs
against a real Postgres — including parity with handwritten DBOS code,
re-runs with zero re-execution, mid-pipeline fork/replay, and one test per
safety guard. Both underlying dependencies
([dbos-transact-golang](https://github.com/dbos-inc/dbos-transact-golang),
[samber/ro](https://github.com/samber/ro)) are pre-1.0, so expect pinned
versions and occasional churn until they stabilize.

Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## Acknowledgements

- Hat tip to [@samber](https://github.com/samber) — duro exists because
  [ro](https://github.com/samber/ro)'s operator model (and its meticulous
  context propagation) turned out to compose beautifully with durable
  execution. If you like `lo`, go look at `ro`.
- The [DBOS](https://www.dbos.dev/) team, whose Postgres-backed durable
  workflow engine does all the heavy lifting here.

## License

[MIT](LICENSE) © Lemonberry Labs
