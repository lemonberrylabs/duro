# Contributing to duro

Thanks for your interest in duro! Contributions of all kinds are welcome —
bug reports, documentation fixes, new tests, and new durable primitives.

## Development setup

You need Go 1.26+ and a local PostgreSQL you can connect to. The tests use a
dedicated database that is wiped on every run:

```bash
createdb duro_test
go test ./... -count=1
```

Point the tests at a different Postgres with `DURO_TEST_DATABASE_URL`
(default: `postgres://$USER@localhost:5432/duro_test`).

Each example app uses its own database — see the README in each directory
(`examples/payments`, `examples/thumbnails`, `examples/housekeeping`). The
orders example:

```bash
createdb duro_demo
cd examples/orders
go run .                              # all demo variants
go run . -variant=duro -crash-after=reserve   # crash mid-workflow…
go run .                              # …and watch it recover
```

## Before opening a PR

```bash
gofmt -l .        # must print nothing
go vet ./...
go test ./... -count=1
```

CI runs exactly these against a real Postgres, so if they pass locally you're
in good shape.

## Guidelines

- **Open an issue first** for behavior changes or new primitives, so we can
  agree on the design before you invest time.
- **Durability is the product.** Any new stage or operator must preserve
  deterministic step ordering under replay. New primitives need tests that
  prove it — see the fork-replay and shape-guard tests in `duro_test.go` for
  the pattern (run, fork mid-pipeline with `dbos.ForkWorkflow`, assert which
  stage functions re-executed via counters).
- **The public API stays closed.** `PipeN` accepts only `Stage` values on
  purpose; don't expose raw `ro.Observable` composition. Escape hatches go
  through explicitly-named constructors (`Pure`, `UnsafeOperator`).
- **Keep dependencies minimal.** The library depends on
  `dbos-transact-golang` and `samber/ro` only; test-only dependencies are fine.
- Small, focused PRs with clear commit messages review fastest.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE).
