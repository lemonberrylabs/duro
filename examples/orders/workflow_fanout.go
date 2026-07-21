package main

import (
	"context"

	"github.com/lemonberrylabs/duro"
)

// pricingQueue bounds how many line items are priced concurrently. Declared
// once and referenced by value; main registers it before launch.
var pricingQueue = duro.NewQueue("pricing", duro.WithConcurrency(2))

// PriceItemWorkflow is the child workflow FanOut spawns per line item. Being
// a workflow, each pricing is independently durable and shows up in DBOS's
// workflow listings with its own ID derived from the parent.
func PriceItemWorkflow(ctx duro.Context, li LineItem) (PricedItem, error) {
	return priceItem(ctx, li)
}

// BatchInvoiceFanOutWorkflow is BatchInvoiceWorkflow with the pricing stage
// parallelized: in-stock items are priced by child workflows on the pricing
// queue (at most 2 at a time), then merged into an Invoice in input order.
func BatchInvoiceFanOutWorkflow(ctx duro.Context, b Batch) (Invoice, error) {
	return duro.Run(ctx, b, duro.Pipe4(
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
		duro.FanOut("price-parallel", pricingQueue, duro.Workflow(PriceItemWorkflow)),
		duro.Reduce("sum-invoice", func(_ context.Context, acc Invoice, p PricedItem) (Invoice, error) {
			acc.TotalCents += p.TotalCents
			acc.ItemCount++
			return acc, nil
		}, Invoice{BatchID: b.ID}),
	))
}
