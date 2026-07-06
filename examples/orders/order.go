package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// Order flows through the demo workflows: validate → reserve → charge → notify.
type Order struct {
	ID          string
	Item        string
	Quantity    int
	AmountCents int
}

type ValidatedOrder struct {
	Order Order
}

type Reservation struct {
	Order         Order
	ReservationID string
}

type Payment struct {
	Reservation  Reservation
	PaymentID    string
	ChargedCents int
}

type Confirmation struct {
	OrderID   string
	PaymentID string
	Message   string
}

// Batch is the input of the complex-pipeline demo: explode → filter → price → sum.
type Batch struct {
	ID    string
	Items []LineItem
}

type LineItem struct {
	SKU       string
	Qty       int
	UnitCents int
	InStock   bool
}

type PricedItem struct {
	SKU        string
	TotalCents int
}

type Invoice struct {
	BatchID    string
	TotalCents int
	ItemCount  int
}

func stepLog(format string, args ...any) {
	fmt.Printf("      [step] "+format+"\n", args...)
}

// --- step functions, shared verbatim by the plain and ro workflow variants ---

func validateOrder(_ context.Context, o Order) (ValidatedOrder, error) {
	if o.Quantity <= 0 {
		return ValidatedOrder{}, fmt.Errorf("order %s: quantity must be positive, got %d", o.ID, o.Quantity)
	}
	if o.AmountCents <= 0 {
		return ValidatedOrder{}, fmt.Errorf("order %s: amount must be positive, got %d", o.ID, o.AmountCents)
	}
	stepLog("validated order %s (%d × %s)", o.ID, o.Quantity, o.Item)
	return ValidatedOrder{Order: o}, nil
}

func reserveInventory(_ context.Context, v ValidatedOrder) (Reservation, error) {
	res := Reservation{Order: v.Order, ReservationID: "rsv-" + v.Order.ID}
	stepLog("reserved inventory for order %s → %s", v.Order.ID, res.ReservationID)
	return res, nil
}

// chargeAttempts tracks per-order attempts so the first charge for each order
// fails with a simulated transient gateway error, demonstrating step retries.
var chargeAttempts sync.Map

func chargePayment(_ context.Context, r Reservation) (Payment, error) {
	count, _ := chargeAttempts.LoadOrStore(r.Order.ID, new(int))
	attempts := count.(*int)
	*attempts++
	if *attempts == 1 {
		stepLog("charging order %s… payment gateway timed out (attempt %d)", r.Order.ID, *attempts)
		return Payment{}, fmt.Errorf("payment gateway timeout for order %s (attempt %d)", r.Order.ID, *attempts)
	}
	p := Payment{Reservation: r, PaymentID: "pay-" + r.Order.ID, ChargedCents: r.Order.AmountCents}
	stepLog("charged %d¢ for order %s → %s (attempt %d)", p.ChargedCents, r.Order.ID, p.PaymentID, *attempts)
	return p, nil
}

func sendConfirmation(_ context.Context, p Payment) (Confirmation, error) {
	c := Confirmation{
		OrderID:   p.Reservation.Order.ID,
		PaymentID: p.PaymentID,
		Message:   fmt.Sprintf("order %s confirmed: %d¢ charged, reservation %s", p.Reservation.Order.ID, p.ChargedCents, p.Reservation.ReservationID),
	}
	stepLog("sent confirmation for order %s", c.OrderID)
	return c, nil
}

func priceItem(_ context.Context, li LineItem) (PricedItem, error) {
	p := PricedItem{SKU: li.SKU, TotalCents: li.Qty * li.UnitCents}
	stepLog("priced %d × %s → %d¢", li.Qty, li.SKU, p.TotalCents)
	// Simulate a slow pricing service so recovery timing is visible in step timestamps.
	time.Sleep(10 * time.Millisecond)
	return p, nil
}

// --- crash simulation for the durability demo ---

var crashAfterStep string

// maybeCrash kills the process when the demo was started with
// -crash-after=<point>. It is called between steps, after the previous step's
// result has been checkpointed, so a restart recovers the workflow and replays
// completed steps instead of re-executing them.
func maybeCrash(point string) {
	if crashAfterStep == point {
		fmt.Printf("\n💥 simulated crash after step %q — run again without -crash-after to watch recovery\n", point)
		os.Exit(1)
	}
}
