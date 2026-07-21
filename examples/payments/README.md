# payments — signals, retries, and human-in-the-loop

A payment processor built from **shared stage values** composed into three
pipelines, showing duro's resilience options and its typed messaging
channels end to end.

```bash
createdb duro_payments
go run .    # or: DBOS_SYSTEM_DATABASE_URL=... go run .
```

## What it demonstrates

| Feature | Where |
|---|---|
| `duro.New` / `Register` / `Start` / `Handle` | `main.go` — the whole app, one import |
| Retry options: `WithMaxRetries`, `WithBaseInterval`, `WithBackoffFactor`, `WithMaxInterval` | `riskCheck` — a flaky service retried with exponential backoff |
| `WithRetryPredicate` | `validate` — a declined card is *permanent*: fails once, no retries burned |
| `WithTimeout` | `riskCheck` — each attempt bounded by a deadline the step context enforces |
| `Delay` | `FastPayment`'s `settle` stage — a durable pause that survives restarts |
| `Topic` + `Recv` + client `Send` | `ReviewedPayment` parks in `await-approval`; `main` releases it with `Approvals.Send(app, runID, approval)` |
| `Event` + `SetEvent` + client `Get` | each pipeline publishes its phase; `main` watches `Progress.Get(...)` turn to `"awaiting-approval"` before approving |
| `Stream` + `ToStream` + `FromStream` + client `Read` | receipts stream out of every run; the `Audit` pipeline drains another run's stream as **one checkpointed step** |
| `Portable()` | the `Receipts` stream is declared portable — a Python/TypeScript DBOS app could consume it |
| Stage reuse | `validate`, `riskCheck`, `charge` are package-level values shared by both payment pipelines |

## The approval pattern

`Recv` emits the received message and **discards the in-flight item** — that
is deliberate (the checkpoint sequence must not depend on data smuggled
around the signal). The idiom, shown in `ReviewedPayment`:

1. `Tap("persist", …)` — durably record the payment (here a map; in
   production, your database).
2. `Recv("await-approval", Approvals, timeout)` — park until a human decides.
   Receipt is checkpointed: a recovered workflow does not consume a second
   message.
3. `Step("reload", …)` — load the payment back by the ID carried in the
   approval, and continue.

The approver is any plain client: `Approvals.Send(app, workflowID, approval)`.
The topic is a typed value, so sending the wrong payload type is a compile
error, not a runtime decode failure.

## Things to try

- Kill the process while the reviewed payment waits for approval, run again,
  and approve: the workflow recovers mid-`Recv` and completes. (`Start` the
  payment with a fixed workflow ID first — `duro.Register` docs — or grab the
  printed run ID.)
- Change `Approvals` to `duro.NewTopic[string]("approvals")` in one place and
  watch the compiler point at every mismatched writer and reader.
- Lower `riskCheck`'s `WithTimeout` below the simulated call latency and
  watch attempts fail with `context.DeadlineExceeded`, then retry.
