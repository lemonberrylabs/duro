package main

import (
	"context"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

// OrderWorkflow is the plain DBOS variant: the same four step functions wired
// with sequential dbos.RunAsStep calls. It is the baseline the ro variant is
// compared against.
func OrderWorkflow(ctx dbos.DBOSContext, o Order) (Confirmation, error) {
	v, err := dbos.RunAsStep(ctx, func(sc context.Context) (ValidatedOrder, error) {
		return validateOrder(sc, o)
	}, dbos.WithStepName("validate"))
	if err != nil {
		return Confirmation{}, err
	}

	r, err := dbos.RunAsStep(ctx, func(sc context.Context) (Reservation, error) {
		return reserveInventory(sc, v)
	}, dbos.WithStepName("reserve"))
	if err != nil {
		return Confirmation{}, err
	}
	maybeCrash("reserve")

	p, err := dbos.RunAsStep(ctx, func(sc context.Context) (Payment, error) {
		return chargePayment(sc, r)
	}, dbos.WithStepName("charge"), dbos.WithStepMaxRetries(3))
	if err != nil {
		return Confirmation{}, err
	}
	maybeCrash("charge")

	return dbos.RunAsStep(ctx, func(sc context.Context) (Confirmation, error) {
		return sendConfirmation(sc, p)
	}, dbos.WithStepName("notify"))
}
