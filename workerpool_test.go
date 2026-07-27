package duro_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lemonberrylabs/duro"
)

// Worker-pool tests run their own executors alongside the shared TestMain app,
// against the same duro_test database. Two things keep them isolated:
//
//   - A distinct application version (wpTestVersion). DBOS's internal-queue
//     dequeue is version-scoped, so the shared app (on its computed version)
//     never picks up a run these tests re-enqueue during takeover.
//   - Distinct, per-test executor IDs, so heartbeat rows and takeover decisions
//     stay attributable across tests sharing the heartbeat table.

const wpTestVersion = "wp-test-v1"

// quietLogger keeps worker-pool test output readable (errors only) — the
// stranded-run warnings each extra executor emits at Launch are just noise here.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// wpConfig returns the fast cadences worker-pool tests run at: a sub-second
// stale threshold so takeover happens within a test's time budget. Production
// values are far higher (see WithStaleThreshold).
func wpFastOptions(extra ...duro.WorkerPoolOption) []duro.WorkerPoolOption {
	base := []duro.WorkerPoolOption{
		duro.WithHeartbeatInterval(150 * time.Millisecond),
		duro.WithStaleThreshold(1 * time.Second),
	}
	return append(base, extra...)
}

// newWorkerPoolApp builds and launches a worker-pool App with fast cadences.
// An empty executorID means "let duro generate one".
func newWorkerPoolApp(t *testing.T, executorID, version string, opts ...duro.WorkerPoolOption) *duro.App {
	t.Helper()
	// Env overrides would collapse every app onto one identity / version.
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	a, err := duro.New(context.Background(), duro.Config{
		Name:               "duro-wp-test",
		DatabaseURL:        testDatabaseURL(),
		Logger:             quietLogger(),
		ExecutorID:         executorID,
		ApplicationVersion: version,
	}, duro.WithWorkerPool(wpFastOptions(opts...)...),
		duro.WithSweepInterval(150*time.Millisecond))
	if err != nil {
		t.Fatalf("New worker-pool app: %v", err)
	}
	if err := a.Launch(); err != nil {
		t.Fatalf("Launch worker-pool app: %v", err)
	}
	return a
}

// wpConn opens a throwaway connection for inspecting duro's heartbeat table and
// DBOS's workflow_status directly.
func wpConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), testDatabaseURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func queryHeartbeat(t *testing.T, conn *pgx.Conn, executorID string) (time.Time, bool) {
	t.Helper()
	var last time.Time
	err := conn.QueryRow(context.Background(),
		`SELECT last_heartbeat FROM dbos.duro_executor_heartbeats WHERE executor_id=$1`, executorID).Scan(&last)
	if err == pgx.ErrNoRows {
		return time.Time{}, false
	}
	if err != nil {
		t.Fatalf("query heartbeat: %v", err)
	}
	return last, true
}

// TestWorkerPoolHeartbeatWritten proves the synchronous first beat: the lease
// row exists immediately after Launch (before any queue runner could dequeue).
func TestWorkerPoolHeartbeatWritten(t *testing.T) {
	execID := "wp-hb-written"
	a := newWorkerPoolApp(t, execID, wpTestVersion)
	defer a.Shutdown(5 * time.Second)

	last, ok := queryHeartbeat(t, wpConn(t), execID)
	if !ok {
		t.Fatalf("no heartbeat row for %q after Launch", execID)
	}
	if age := time.Since(last); age > 5*time.Second {
		t.Errorf("heartbeat stale right after Launch: age=%v", age)
	}
}

// TestWorkerPoolHeartbeatAdvances proves the periodic beat: last_heartbeat keeps
// moving forward while the process is alive.
func TestWorkerPoolHeartbeatAdvances(t *testing.T) {
	execID := "wp-hb-advances"
	a := newWorkerPoolApp(t, execID, wpTestVersion)
	defer a.Shutdown(5 * time.Second)
	conn := wpConn(t)

	first, ok := queryHeartbeat(t, conn, execID)
	if !ok {
		t.Fatal("no initial heartbeat")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		if later, _ := queryHeartbeat(t, conn, execID); later.After(first) {
			return
		}
	}
	t.Errorf("heartbeat did not advance from %v within timeout", first)
}

// TestWorkerPoolTombstoneOnShutdown proves graceful shutdown tombstones the
// lease (far-past timestamp) rather than deleting it — an absent row means
// "unknown", a tombstone means "known-dead, adopt now".
func TestWorkerPoolTombstoneOnShutdown(t *testing.T) {
	execID := "wp-tombstone"
	a := newWorkerPoolApp(t, execID, wpTestVersion)
	conn := wpConn(t)
	if _, ok := queryHeartbeat(t, conn, execID); !ok {
		t.Fatal("no heartbeat before shutdown")
	}

	a.Shutdown(5 * time.Second)

	last, ok := queryHeartbeat(t, conn, execID)
	if !ok {
		t.Fatal("heartbeat row removed on shutdown; want a tombstone")
	}
	if time.Since(last) < time.Hour {
		t.Errorf("heartbeat not tombstoned: last=%v (age %v)", last, time.Since(last))
	}
}

// TestWorkerPoolGeneratesUniqueExecutorID proves worker-pool mode mints a
// distinct identity per process when none is configured — the precondition for
// executors ever recognizing each other as separate.
func TestWorkerPoolGeneratesUniqueExecutorID(t *testing.T) {
	a := newWorkerPoolApp(t, "", wpTestVersion)
	defer a.Shutdown(5 * time.Second)
	b := newWorkerPoolApp(t, "", wpTestVersion)
	defer b.Shutdown(5 * time.Second)

	idA, idB := a.Context().GetExecutorID(), b.Context().GetExecutorID()
	if idA == "" || idA == "local" {
		t.Errorf("executor A id = %q, want a generated unique id", idA)
	}
	if idA == idB {
		t.Errorf("both apps got the same generated executor id %q", idA)
	}
}

// TestWorkerPoolExplicitExecutorID proves an explicit ExecutorID always wins
// over generation.
func TestWorkerPoolExplicitExecutorID(t *testing.T) {
	a := newWorkerPoolApp(t, "explicit-exec-1", wpTestVersion)
	defer a.Shutdown(5 * time.Second)
	if got := a.Context().GetExecutorID(); got != "explicit-exec-1" {
		t.Errorf("executor id = %q, want explicit %q", got, "explicit-exec-1")
	}
}
