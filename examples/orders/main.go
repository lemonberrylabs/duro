package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

const (
	crashDemoWorkflowIDPlain = "order-crash-plain"
	crashDemoWorkflowIDDuro  = "order-crash-duro"
)

func main() {
	variant := flag.String("variant", "all", "demo to run: plain | duro | batch | all")
	crashAfter := flag.String("crash-after", "", "simulate a crash after the named order step (reserve or charge); rerun without this flag to watch recovery")
	flag.Parse()
	crashAfterStep = *crashAfter

	dctx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:     "duro-orders",
		DatabaseURL: databaseURL(),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal("initializing DBOS: %v", err)
	}

	dbos.RegisterWorkflow(dctx, OrderWorkflow, dbos.WithWorkflowName("OrderWorkflow"))
	dbos.RegisterWorkflow(dctx, OrderWorkflowRo, dbos.WithWorkflowName("OrderWorkflowRo"))
	dbos.RegisterWorkflow(dctx, BatchInvoiceWorkflow, dbos.WithWorkflowName("BatchInvoiceWorkflow"))

	if err := dbos.Launch(dctx); err != nil {
		fatal("launching DBOS: %v", err)
	}
	defer dbos.Shutdown(dctx, 5*time.Second)

	// A crashed demo from a previous run is recovered automatically at Launch;
	// report it before running anything new.
	if crashAfterStep == "" {
		reportRecoveredCrashDemos(dctx)
	}

	if crashAfterStep != "" {
		runCrashDemo(dctx, *variant)
		return // unreachable when the crash fires; kept for odd step names
	}

	runID := time.Now().UnixMilli()

	if *variant == "plain" || *variant == "all" {
		order := Order{ID: fmt.Sprintf("plain-%d", runID), Item: "espresso machine", Quantity: 1, AmountCents: 79900}
		section("Plain DBOS variant — sequential RunAsStep calls")
		runOrder(dctx, OrderWorkflow, order)
	}

	if *variant == "duro" || *variant == "all" {
		order := Order{ID: fmt.Sprintf("duro-%d", runID), Item: "burr grinder", Quantity: 2, AmountCents: 24900}
		section("duro variant — pipeline of durable stages (ro under the hood)")
		runOrder(dctx, OrderWorkflowRo, order)
	}

	if *variant == "batch" || *variant == "all" {
		section("Complex duro pipeline — Expand → Filter → Step → Tap → Reduce")
		batch := Batch{
			ID: fmt.Sprintf("batch-%d", runID),
			Items: []LineItem{
				{SKU: "beans-1kg", Qty: 3, UnitCents: 1800, InStock: true},
				{SKU: "filter-papers", Qty: 10, UnitCents: 450, InStock: true},
				{SKU: "gold-tamper", Qty: 1, UnitCents: 9900, InStock: false},
				{SKU: "milk-jug", Qty: 2, UnitCents: 2200, InStock: true},
			},
		}
		handle, err := dbos.RunWorkflow(dctx, BatchInvoiceWorkflow, batch)
		if err != nil {
			fatal("starting batch workflow: %v", err)
		}
		invoice, err := handle.GetResult()
		if err != nil {
			fatal("batch workflow failed: %v", err)
		}
		fmt.Printf("   result: invoice for %s — %d items, %d¢ total\n", invoice.BatchID, invoice.ItemCount, invoice.TotalCents)
		printSteps(dctx, handle.GetWorkflowID())
	}
}

func runOrder(dctx dbos.DBOSContext, wf dbos.Workflow[Order, Confirmation], order Order, opts ...dbos.WorkflowOption) {
	handle, err := dbos.RunWorkflow(dctx, wf, order, opts...)
	if err != nil {
		fatal("starting order workflow: %v", err)
	}
	confirmation, err := handle.GetResult()
	if err != nil {
		fatal("order workflow failed: %v", err)
	}
	fmt.Printf("   result: %s\n", confirmation.Message)
	printSteps(dctx, handle.GetWorkflowID())
}

// runCrashDemo starts an order workflow with a fixed workflow ID and lets
// maybeCrash kill the process after the requested step's checkpoint. A
// completed demo workflow from an earlier crash/recovery cycle is deleted
// first so the demo is repeatable — otherwise DBOS would return its recorded
// result instead of executing.
func runCrashDemo(dctx dbos.DBOSContext, variant string) {
	wf, wfID := dbos.Workflow[Order, Confirmation](OrderWorkflowRo), crashDemoWorkflowIDDuro
	if variant == "plain" {
		wf, wfID = OrderWorkflow, crashDemoWorkflowIDPlain
	}
	if err := dbos.DeleteWorkflows(dctx, []string{wfID}); err != nil {
		fatal("deleting previous crash-demo workflow %s: %v", wfID, err)
	}
	section(fmt.Sprintf("Crash demo — will exit after step %q (workflow %s)", crashAfterStep, wfID))
	runOrder(dctx, wf, Order{ID: "crash-demo", Item: "unbreakable mug", Quantity: 1, AmountCents: 1500}, dbos.WithWorkflowID(wfID))
}

// reportRecoveredCrashDemos waits for crash-demo workflows left PENDING by a
// previous run; DBOS re-executes them on Launch, replaying completed steps
// from their checkpoints.
func reportRecoveredCrashDemos(dctx dbos.DBOSContext) {
	pending, err := dbos.ListWorkflows(dctx,
		dbos.WithWorkflowIDPrefix("order-crash-"),
		dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusPending}),
	)
	if err != nil || len(pending) == 0 {
		return
	}
	for _, wf := range pending {
		section(fmt.Sprintf("Recovering crashed workflow %s — completed steps replay from checkpoints", wf.ID))
		handle, err := dbos.RetrieveWorkflow[Confirmation](dctx, wf.ID)
		if err != nil {
			fatal("retrieving crashed workflow %s: %v", wf.ID, err)
		}
		confirmation, err := handle.GetResult()
		if err != nil {
			fmt.Printf("   recovered workflow failed: %v\n", err)
			continue
		}
		fmt.Printf("   result: %s\n", confirmation.Message)
		printSteps(dctx, wf.ID)
		fmt.Println("   note: steps checkpointed before the crash keep their original timestamps —")
		fmt.Println("         only the steps after the crash ran in this process.")
	}
}

func printSteps(dctx dbos.DBOSContext, workflowID string) {
	steps, err := dbos.GetWorkflowSteps(dctx, workflowID)
	if err != nil {
		fatal("fetching steps for %s: %v", workflowID, err)
	}
	fmt.Printf("   recorded steps for %s:\n", workflowID)
	for _, s := range steps {
		status := "✓"
		if s.Error != nil {
			status = "✗ " + s.Error.Error()
		}
		fmt.Printf("      %2d  %-14s %s  (completed %s)\n", s.StepID, s.StepName, status, s.CompletedAt.Format("15:04:05.000"))
	}
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_demo", username)
}

func section(title string) {
	fmt.Printf("\n── %s\n", title)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
