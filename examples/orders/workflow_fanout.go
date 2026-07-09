package main

import (
	"context"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// pricingQueueName is the DBOS queue that bounds how many line items are
// priced concurrently. Registered in main with a global concurrency of 2.
const pricingQueueName = "pricing"

// PriceItemWorkflow is the child workflow FanOut spawns per line item. Being
// a workflow, each pricing is independently durable and shows up in DBOS's
// workflow listings with its own ID derived from the parent.
func PriceItemWorkflow(ctx dbos.DBOSContext, li LineItem) (PricedItem, error) {
	return priceItem(ctx, li)
}

// BatchInvoiceFanOutWorkflow is BatchInvoiceWorkflow with the pricing stage
// parallelized: in-stock items are priced by child workflows on the pricing
// queue (at most 2 at a time), then merged into an Invoice in input order.
func BatchInvoiceFanOutWorkflow(ctx dbos.DBOSContext, b Batch) (Invoice, error) {
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
		duro.FanOut("price-parallel", pricingQueueName, PriceItemWorkflow),
		duro.Reduce("sum-invoice", func(_ context.Context, acc Invoice, p PricedItem) (Invoice, error) {
			acc.TotalCents += p.TotalCents
			acc.ItemCount++
			return acc, nil
		}, Invoice{BatchID: b.ID}),
	))
}
