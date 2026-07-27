package duro

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// White-box tests for the stale-run warning (R4) and retention (R5). Both drive
// the checks directly against seeded workflow_status rows. Isolation trick: seed
// rows with timestamps hours in the past and use a one-hour window, so only the
// seeded rows match — every real run this suite creates is seconds old.

// newMaintenanceApp builds (without launching) a maintenance-only App — no
// worker-pool takeover, just the pool and DBOS context the R4/R5 checks use.
func newMaintenanceApp(t *testing.T, version string) *App {
	t.Helper()
	t.Setenv("DBOS__APPVERSION", "")
	a, err := New(context.Background(), Config{
		Name:               "duro-maint-" + t.Name(),
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ApplicationVersion: version,
	}, WithStaleRunWarning(time.Hour), WithRetention(time.Hour))
	if err != nil {
		t.Fatalf("New maintenance app: %v", err)
	}
	// These tests drive the checks directly rather than launching (which would
	// start the periodic loop). Give the worker pool the maintenance context its
	// methods bound their per-operation timeouts to.
	a.wp.maintCtx, a.wp.maintCancel = context.WithCancel(context.Background())
	t.Cleanup(func() { a.Shutdown(5 * time.Second) })
	return a
}

// seedRun inserts a synthetic workflow_status row with explicit created_at and
// (optionally) completed_at, so time-window filters can be exercised
// deterministically. A zero completedAt leaves completed_at NULL.
func seedRun(t *testing.T, pool *pgxpool.Pool, id, status, version string, createdAt, completedAt time.Time) {
	t.Helper()
	var completedMs *int64
	if !completedAt.IsZero() {
		ms := completedAt.UnixMilli()
		completedMs = &ms
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO dbos.workflow_status (workflow_uuid, name, status, application_version, created_at, updated_at, completed_at)
		 VALUES ($1,$2,$3,$4,$5,$5,$6)
		 ON CONFLICT (workflow_uuid) DO UPDATE SET
		   status=EXCLUDED.status, application_version=EXCLUDED.application_version,
		   created_at=EXCLUDED.created_at, completed_at=EXCLUDED.completed_at`,
		id, "seeded-"+id, status, version, createdAt.UnixMilli(), completedMs)
	if err != nil {
		t.Fatalf("seed run %s: %v", id, err)
	}
}

func rowExists(t *testing.T, pool *pgxpool.Pool, id string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM dbos.workflow_status WHERE workflow_uuid=$1", id).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", id, err)
	}
	return n > 0
}

// TestStaleRunWarning proves the R4 split: runs older than the age that are
// still waiting or running are counted, split by whether this executor's version
// can recover them, while recent, terminal, and DELAYED runs are ignored — and
// the hook receives the same counts.
//
// Counts are asserted as deltas around the seed. checkStaleRuns aggregates over
// the whole database (there is no app column to scope by), so any other test's
// old non-terminal rows would otherwise leak into the totals.
func TestStaleRunWarning(t *testing.T) {
	const ver = "r4-same-version-unique"
	app := newMaintenanceApp(t, ver)
	pool := app.wp.pool
	app.wp.staleRunWarn = time.Hour

	before, err := app.wp.checkStaleRuns()
	if err != nil {
		t.Fatalf("baseline checkStaleRuns: %v", err)
	}

	old := time.Now().Add(-2 * time.Hour)
	now := time.Now()
	seedRun(t, pool, "r4-same-1", "PENDING", ver, old, time.Time{})
	seedRun(t, pool, "r4-same-2", "ENQUEUED", ver, old, time.Time{})
	seedRun(t, pool, "r4-other-1", "PENDING", "r4-other-a", old, time.Time{})
	seedRun(t, pool, "r4-other-2", "ENQUEUED", "r4-other-b", old, time.Time{})
	seedRun(t, pool, "r4-recent", "PENDING", ver, now, time.Time{})   // too new
	seedRun(t, pool, "r4-terminal", "SUCCESS", ver, old, time.Time{}) // terminal
	seedRun(t, pool, "r4-delayed", "DELAYED", ver, old, time.Time{})  // parked by design (debounce)
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r4-%'")
	})

	info, err := app.wp.checkStaleRuns()
	if err != nil {
		t.Fatalf("checkStaleRuns: %v", err)
	}
	if got := info.SameVersion - before.SameVersion; got != 2 {
		t.Errorf("SameVersion delta = %d, want 2 (DELAYED and recent/terminal runs must not count)", got)
	}
	if got := info.OtherVersion - before.OtherVersion; got != 2 {
		t.Errorf("OtherVersion delta = %d, want 2", got)
	}

	// The hook receives the same counts when there is something to report.
	var got StaleRunInfo
	app.wp.staleRunHook = func(i StaleRunInfo) { got = i }
	app.wp.runStaleRunWarning()
	if got != info {
		t.Errorf("hook got %+v, want %+v", got, info)
	}

	// No warning (hook untouched) when nothing is that old: a 100h window is
	// older than any run the seed created (all ≤2h old).
	got = StaleRunInfo{}
	app.wp.staleRunWarn = 100 * time.Hour
	app.wp.runStaleRunWarning()
	if got != (StaleRunInfo{}) {
		t.Errorf("hook called for a 100h window with no runs that old: %+v", got)
	}
}

// TestMaintenanceLoopWithoutWorkerPool proves retention and the stale-run
// warning work on their own: an app with neither WithWorkerPool nor a heartbeat
// lease still launches, runs its maintenance cycle on the sweep cadence, and
// deletes expired runs — the no-worker-pool branch of startMaintenance, which
// the direct-call tests above never execute.
func TestMaintenanceLoopWithoutWorkerPool(t *testing.T) {
	t.Setenv("DBOS__APPVERSION", "")
	warned := make(chan StaleRunInfo, 1)
	app, err := New(context.Background(), Config{
		Name:               "duro-maint-loop",
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ApplicationVersion: "maint-loop-v1",
	},
		WithRetention(time.Hour),
		WithStaleRunWarning(time.Hour, func(i StaleRunInfo) {
			select {
			case warned <- i:
			default:
			}
		}),
		// A fast cadence so the loop cycles within the test's budget —
		// configurable here precisely because WithSweepInterval is a plain
		// Option, usable without a worker pool.
		WithSweepInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	pool := app.wp.pool

	old := time.Now().Add(-2 * time.Hour)
	seedRun(t, pool, "r45-expired", "SUCCESS", "maint-loop-v1", old, old)
	seedRun(t, pool, "r45-limbo", "PENDING", "maint-loop-v1", old, time.Time{})
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r45-%'")
	})

	if err := app.Launch(); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	defer app.Shutdown(5 * time.Second)

	// Worker-pool mode is off, so no lease was written — and the table may not
	// even exist yet (nothing in this app creates it), which is proof in itself.
	var tableExists bool
	if err := pool.QueryRow(context.Background(),
		"SELECT to_regclass('dbos.duro_executor_heartbeats') IS NOT NULL").Scan(&tableExists); err != nil {
		t.Fatalf("checking for the heartbeat table: %v", err)
	}
	if tableExists {
		var leases int
		if err := pool.QueryRow(context.Background(),
			"SELECT count(*) FROM dbos.duro_executor_heartbeats WHERE app_name='duro-maint-loop'").Scan(&leases); err != nil {
			t.Fatalf("counting leases: %v", err)
		}
		if leases != 0 {
			t.Errorf("maintenance-only app wrote %d heartbeat leases, want 0", leases)
		}
	}

	// The loop runs both duties unprompted.
	select {
	case info := <-warned:
		if info.SameVersion < 1 {
			t.Errorf("stale-run warning reported %+v, want at least 1 same-version run", info)
		}
	case <-time.After(10 * time.Second):
		t.Error("stale-run warning never fired from the maintenance loop")
	}
	deadline := time.Now().Add(10 * time.Second)
	for rowExists(t, pool, "r45-expired") {
		if time.Now().After(deadline) {
			t.Fatal("retention never deleted the expired run from the maintenance loop")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !rowExists(t, pool, "r45-limbo") {
		t.Error("retention deleted a non-terminal run")
	}
}

// TestOptionValidation proves cadence mistakes are rejected by New rather than
// panicking a ticker in a background goroutine after Launch already succeeded,
// or silently disabling the feature the caller asked for.
func TestOptionValidation(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
		want string
	}{
		{"zero heartbeat", []Option{WithWorkerPool(WithHeartbeatInterval(0))}, "WithHeartbeatInterval"},
		{"negative sweep", []Option{WithWorkerPool(), WithSweepInterval(-time.Second)}, "WithSweepInterval"},
		{"negative sweep without a worker pool", []Option{WithRetention(time.Hour), WithSweepInterval(-time.Second)}, "WithSweepInterval"},
		{"zero stale threshold", []Option{WithWorkerPool(WithStaleThreshold(0))}, "WithStaleThreshold"},
		{
			"stale threshold too close to the beat",
			[]Option{WithWorkerPool(WithHeartbeatInterval(time.Second), WithStaleThreshold(time.Second))},
			"at least 2×",
		},
		{"zero retention", []Option{WithRetention(0)}, "WithRetention"},
		{"negative stale-run age", []Option{WithStaleRunWarning(-time.Minute)}, "WithStaleRunWarning"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := New(context.Background(), Config{
				Name:        "duro-validate",
				DatabaseURL: wpURL(),
				Logger:      wpLogger(),
			}, tc.opts...)
			if err == nil {
				app.Shutdown(time.Second)
				t.Fatalf("New accepted %s, want an error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}

	// A valid worker-pool configuration still constructs.
	app, err := New(context.Background(), Config{
		Name:        "duro-validate-ok",
		DatabaseURL: wpURL(),
		Logger:      wpLogger(),
	}, WithWorkerPool(WithHeartbeatInterval(time.Second), WithStaleThreshold(10*time.Second)))
	if err != nil {
		t.Fatalf("New rejected a valid configuration: %v", err)
	}
	app.Shutdown(time.Second)
}

// TestShutdownIsIdempotent proves a second Shutdown is a no-op rather than
// tombstoning against a closed pool.
func TestShutdownIsIdempotent(t *testing.T) {
	t.Setenv("DBOS__VMID", "")
	t.Setenv("DBOS__APPVERSION", "")
	app, err := New(context.Background(), Config{
		Name:               "duro-double-shutdown",
		DatabaseURL:        wpURL(),
		Logger:             wpLogger(),
		ExecutorID:         "double-shutdown-exec",
		ApplicationVersion: "double-shutdown-v1",
	}, WithWorkerPool(WithHeartbeatInterval(200*time.Millisecond), WithStaleThreshold(2*time.Second)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := app.Launch(); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	app.Shutdown(2 * time.Second)
	app.Shutdown(2 * time.Second) // must not panic or touch the closed pool
}

// TestRetention proves R5: terminal runs that completed before the window are
// deleted; recent-terminal and non-terminal runs are kept.
func TestRetention(t *testing.T) {
	app := newMaintenanceApp(t, "r5-version")
	pool := app.wp.pool

	old := time.Now().Add(-2 * time.Hour)
	now := time.Now()
	seedRun(t, pool, "r5-old-success", "SUCCESS", "r5-version", old, old)
	seedRun(t, pool, "r5-old-error", "ERROR", "r5-version", old, old)
	seedRun(t, pool, "r5-recent-success", "SUCCESS", "r5-version", now, now)
	seedRun(t, pool, "r5-pending-old", "PENDING", "r5-version", old, time.Time{})
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DELETE FROM dbos.workflow_status WHERE workflow_uuid LIKE 'r5-%'")
	})

	app.wp.retention = time.Hour
	n, err := app.wp.sweepRetention()
	if err != nil {
		t.Fatalf("sweepRetention: %v", err)
	}
	if n < 2 {
		t.Errorf("deleted %d runs, want at least the 2 expired ones", n)
	}
	if rowExists(t, pool, "r5-old-success") {
		t.Error("r5-old-success survived retention (completed 2h ago, window 1h)")
	}
	if rowExists(t, pool, "r5-old-error") {
		t.Error("r5-old-error survived retention")
	}
	if !rowExists(t, pool, "r5-recent-success") {
		t.Error("r5-recent-success was deleted (completed just now)")
	}
	if !rowExists(t, pool, "r5-pending-old") {
		t.Error("r5-pending-old was deleted (non-terminal must be kept)")
	}
}
