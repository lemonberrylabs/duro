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
func OrderWorkflow(ctx dbos.DBOSContext, o Order) (Confirmation, error) {
	return duro.Run(ctx, o, duro.Pipe4(
		duro.Step("validate", validateOrder),
		duro.Step("reserve", reserveInventory),
		duro.Step("charge", chargePayment, duro.WithMaxRetries(3)),
		duro.Step("notify", sendConfirmation),
	))
}
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
- **Tiny surface** — seven stage constructors, `Pipe1`…`Pipe8`, `Run`/`RunAll`.
  That's the whole API.

## Installation

```bash
go get github.com/lemonberrylabs/duro
```

Requires Go 1.26+ and PostgreSQL (DBOS's system database).

## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
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

func ChargeOrder(ctx dbos.DBOSContext, o Order) (Receipt, error) {
	return duro.Run(ctx, o, duro.Pipe2(
		duro.Step("charge", func(_ context.Context, o Order) (string, error) {
			return "pay-" + o.ID, nil // call your payment provider here
		}, duro.WithMaxRetries(3)),
		duro.Step("receipt", func(_ context.Context, paymentID string) (Receipt, error) {
			return Receipt{OrderID: o.ID, PaymentID: paymentID}, nil
		}),
	))
}

func main() {
	ctx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:     "quickstart",
		DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
	})
	if err != nil {
		panic(err)
	}
	dbos.RegisterWorkflow(ctx, ChargeOrder)
	if err := dbos.Launch(ctx); err != nil {
		panic(err)
	}
	defer dbos.Shutdown(ctx, 5*time.Second)

	handle, err := dbos.RunWorkflow(ctx, ChargeOrder, Order{ID: "42", AmountCents: 1999})
	if err != nil {
		panic(err)
	}
	receipt, err := handle.GetResult()
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
| `duro.FanOut(name, queue, wf)` | parallel map: each item runs as a child workflow on a DBOS queue | ✅ every child is a durable workflow |
| `duro.Pure(name, fn)` | cheap reshaping between stages | ❌ re-executes on replay — must be deterministic and side-effect free |
| `duro.UnsafeOperator(name, op)` | escape hatch for raw ro operators | ⚠️ you're on your own (runtime guards still apply) |

Stages compose with `duro.Pipe1`…`Pipe8` into a `Pipeline[P, R]`, and run with:

- `duro.Run(ctx, input, pipeline)` — returns the last emitted value
- `duro.RunAll(ctx, input, pipeline)` — returns every emitted value

Per-stage retries with exponential backoff: `duro.WithMaxRetries(n)`,
`duro.WithBaseInterval(d)`.

### Multi-item pipelines

Durability per item, not just per workflow — each stage execution below is its
own checkpoint, so a crash mid-batch resumes at the exact item and stage where
it stopped:

```go
func InvoiceWorkflow(ctx dbos.DBOSContext, b Batch) (Invoice, error) {
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
// At startup:
dbos.RegisterQueue(dctx, "jobs", dbos.WithGlobalConcurrency(4))

func ProcessAll(ctx dbos.DBOSContext, jobs []Job) (Merged, error) {
	return duro.Run(ctx, jobs, duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, js []Job) ([]Job, error) { return js, nil }),
		duro.FanOut("process", "jobs", ProcessJob), // ProcessJob: a registered dbos.Workflow
		duro.Reduce("merge", mergeResults, Merged{}),
	))
}
```

Children are enqueued in stream order with IDs derived from the parent's step
counter, so a recovered parent re-attaches to its children instead of spawning
duplicates; results are awaited and emitted in input order, each checkpointed
in the parent. This is the sanctioned form of concurrency inside a pipeline —
determinism is preserved because DBOS checkpoints both the spawns and the
awaits.

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

`dbos.DBOSContext` implements `context.Context`, and
[samber/ro](https://github.com/samber/ro) — the reactive engine under duro's
hood — propagates the subscription context unchanged through its operators.
`duro.Run` subscribes the composed pipeline with the workflow's context; each
stage recovers it and executes its function via `dbos.RunAsStep`, one durable
checkpoint per stage execution. Synchronous emission on the workflow goroutine
keeps step order deterministic — exactly what DBOS replay requires.

## Example app

A complete demo lives in [`examples/orders`](examples/orders): the same order
workflow written both as plain sequential DBOS steps and as a duro pipeline
(their recorded checkpoints are identical), a complex batch pipeline, and a
crash-recovery demo:

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
