package duro_test

import (
	"context"

	"github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/lemonberrylabs/duro"
)

// Example shows a DBOS workflow written as a durable pipeline: each stage
// runs as a checkpointed DBOS step, so a crashed workflow resumes after the
// last completed stage. Register the workflow with dbos.RegisterWorkflow and
// start it with dbos.RunWorkflow as usual.
func Example() {
	type Order struct {
		ID          string
		AmountCents int
	}
	type Receipt struct {
		OrderID   string
		PaymentID string
	}

	chargeOrder := func(ctx dbos.DBOSContext, o Order) (Receipt, error) {
		return duro.Run(ctx, o, duro.Pipe2(
			duro.Step("charge", func(_ context.Context, o Order) (string, error) {
				return "pay-" + o.ID, nil // call your payment provider here
			}, duro.WithMaxRetries(3)),
			duro.Step("receipt", func(_ context.Context, paymentID string) (Receipt, error) {
				return Receipt{OrderID: o.ID, PaymentID: paymentID}, nil
			}),
		))
	}
	_ = chargeOrder
}
