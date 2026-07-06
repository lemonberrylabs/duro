package main

import (
	"context"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// crashProbe is a non-durable, deterministic pass-through stage used only by
// the crash demo. duro.Pure stages re-execute on every replay, which is
// exactly what the demo needs: the probe fires only in the process where the
// -crash-after flag is set.
func crashProbe[T any](point string) duro.Stage[T, T] {
	return duro.Pure("crash-probe-"+point, func(v T) T {
		maybeCrash(point)
		return v
	})
}

// OrderWorkflowRo is the duro variant of OrderWorkflow: the same four step
// functions composed as a durable pipeline. Each duro.Step runs inside
// dbos.RunAsStep, so checkpointing and recovery behave exactly like the plain
// variant.
func OrderWorkflowRo(ctx dbos.DBOSContext, o Order) (Confirmation, error) {
	return duro.Run(ctx, o, duro.Pipe6(
		duro.Step("validate", validateOrder),
		duro.Step("reserve", reserveInventory),
		crashProbe[Reservation]("reserve"),
		duro.Step("charge", chargePayment, duro.WithMaxRetries(3)),
		crashProbe[Payment]("charge"),
		duro.Step("notify", sendConfirmation),
	))
}

// BatchInvoiceWorkflow shows a more complex durable pipeline built from
// duro primitives: one Batch is exploded into line items, filtered, priced,
// audited, and folded into an Invoice — every stage a checkpointed DBOS step.
func BatchInvoiceWorkflow(ctx dbos.DBOSContext, b Batch) (Invoice, error) {
	return duro.Run(ctx, b, duro.Pipe5(
		duro.Expand("explode-batch", func(_ context.Context, b Batch) ([]LineItem, error) {
			stepLog("exploding batch %s into %d line items", b.ID, len(b.Items))
			return b.Items, nil
		}),
		duro.Filter("in-stock", func(_ context.Context, li LineItem) (bool, error) {
			if !li.InStock {
				stepLog("skipping %s: out of stock", li.SKU)
			}
			return li.InStock, nil
		}),
		duro.Step("price-item", priceItem),
		duro.Tap("audit", func(_ context.Context, p PricedItem) error {
			stepLog("audit: %s priced at %d¢", p.SKU, p.TotalCents)
			return nil
		}),
		duro.Reduce("sum-invoice", func(_ context.Context, acc Invoice, p PricedItem) (Invoice, error) {
			acc.TotalCents += p.TotalCents
			acc.ItemCount++
			return acc, nil
		}, Invoice{BatchID: b.ID}),
	))
}
