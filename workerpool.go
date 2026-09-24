package duro

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Worker-pool mode makes a fleet of duro processes recover each other's runs
// without DBOS Conductor. Each process heartbeats a lease row; a sweeper on
// every process re-enqueues the PENDING runs of any process whose lease has
// gone stale, so a worker killed on ephemeral infrastructure (Cloud Run, ECS,
// spot instances) does not strand its in-flight work. Enable it with
// WithWorkerPool; tune it with the WorkerPoolOptions below.
//
// The mechanism is liveness-gated, never launch-gated: a run is taken over only
// because its owner's heartbeat is stale, and only by an executor on the same
// application version (a new deploy never adopts an old version's runs — see
// Config.ApplicationVersion and WithStaleRunWarning). Takeover guarantees
// exactly-once workflow completion but, like all DBOS recovery, at-least-once
// step side effects — keep steps idempotent, and keep the stale threshold well
// above the heartbeat interval plus any worst-case GC pause.

const (
	defaultHeartbeatInterval = 10 * time.Second
	defaultStaleThreshold    = 60 * time.Second
	defaultSweepInterval     = 30 * time.Second

	// opTimeout bounds every background database operation — duro's own pgx
	// queries and the DBOS-routed ones alike — so a hung connection frees the
	// maintenance goroutine instead of blocking it forever (which would silently
	// end takeover sweeps on this process). It never affects staleness: a
	// heartbeat that times out simply does not commit, so the row ages naturally.
	// Generous on purpose — the operations take milliseconds.
	opTimeout = 30 * time.Second

	// tombstoneTimeout bounds the shutdown tombstone, and is deliberately far
	// tighter than opTimeout. The tombstone is the one operation that runs after
	// DBOS shutdown, past the point App.Close's timeout covers, so every second
	// it takes overruns the process's SIGTERM budget. It is a single-row
	// primary-key UPDATE: if it cannot land in this long the database is
	// unhealthy, and waiting longer buys nothing the stale-threshold fallback
	// does not already cover.
	tombstoneTimeout = 5 * time.Second

	// heartbeatTable is duro's own liveness table, created in the dbos schema
	// alongside DBOS's tables so it is dropped and backed up together with them.
	heartbeatTable = "dbos.duro_executor_heartbeats"

	// internalQueueName is DBOS's internal queue (dbos queue.go
	// _DBOS_INTERNAL_QUEUE_NAME), which every process runs a queue runner for. A
	// taken-over run that has no queue of its own — a directly started pipeline —
	// is re-enqueued here, where any live worker picks it up. Runs that came from
	// a queue go back on theirs; see takeoverSQL.
	internalQueueName = "_dbos_internal_queue"
)

// querier is the subset of pgx used by the takeover UPDATE; both *pgxpool.Pool
// and pgx.Tx satisfy it.
type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// StaleRunInfo reports the counts a stale-run warning surfaces on each sweep
// (see WithStaleRunWarning). Stranded runs otherwise emit no signal at all.
type StaleRunInfo struct {
	// SameVersion counts non-terminal runs older than the warning age that this
	// executor's application version can run — in limbo, but recoverable by
	// this fleet. Runs recorded with no version (a Client's default enqueue)
	// count here while this version is the latest registered one: DBOS hands
	// them to the latest version only.
	SameVersion int
	// OtherVersion counts non-terminal runs older than the warning age that
	// this version cannot run: runs on other application versions, and runs
	// with no version while another version is the latest registered — not
	// recoverable here until an executor on the right version runs (see
	// Config.ApplicationVersion).
	OtherVersion int
}

// appOptions accumulates the options passed to New.
type appOptions struct {
	workerPool        bool
	heartbeatInterval time.Duration
	staleThreshold    time.Duration
	sweepInterval     time.Duration
	retention         time.Duration      // 0 = disabled (R5)
	staleRunWarn      time.Duration      // 0 = disabled (R4)
	staleRunHook      func(StaleRunInfo) // optional (R4)

	// *Set records that the caller passed the option explicitly, so an explicit
	// zero (which would silently disable the very feature being asked for) is
	// rejected rather than treated as "not configured".
	retentionSet    bool
	staleRunWarnSet bool
}

func defaultAppOptions() appOptions {
	return appOptions{
		heartbeatInterval: defaultHeartbeatInterval,
		staleThreshold:    defaultStaleThreshold,
		sweepInterval:     defaultSweepInterval,
	}
}

// minStaleRatio is the smallest stale-threshold-to-heartbeat ratio duro accepts.
// Below it a merely slow process (one missed beat) looks dead, and its live runs
// are taken over while still executing — the concurrent double execution
// worker-pool mode exists to prevent. Two is the floor; production wants far
// more headroom (the defaults are 6×).
const minStaleRatio = 2

// validate rejects option combinations that would fail late — in a background
// goroutine, after New and Launch have already returned success — or that would
// silently corrupt the liveness signal. Cadence mistakes are configuration
// errors, so they surface from New rather than as a documented hazard.
func (o appOptions) validate() error {
	if o.sweepInterval <= 0 {
		return fmt.Errorf("duro: WithSweepInterval requires a positive interval, got %v", o.sweepInterval)
	}
	if o.workerPool {
		if o.heartbeatInterval <= 0 {
			return fmt.Errorf("duro: WithHeartbeatInterval requires a positive interval, got %v", o.heartbeatInterval)
		}
		if o.staleThreshold <= 0 {
			return fmt.Errorf("duro: WithStaleThreshold requires a positive threshold, got %v", o.staleThreshold)
		}
		if o.staleThreshold < minStaleRatio*o.heartbeatInterval {
			return fmt.Errorf("duro: WithStaleThreshold (%v) must be at least %d× WithHeartbeatInterval (%v): a threshold that close to the beat marks live executors dead and runs their in-flight work a second time elsewhere",
				o.staleThreshold, minStaleRatio, o.heartbeatInterval)
		}
	}
	if o.retentionSet && o.retention <= 0 {
		return fmt.Errorf("duro: WithRetention requires a positive duration, got %v", o.retention)
	}
	if o.staleRunWarnSet && o.staleRunWarn <= 0 {
		return fmt.Errorf("duro: WithStaleRunWarning requires a positive age, got %v", o.staleRunWarn)
	}
	return nil
}

// Option configures a duro App; pass Options to New after the Config.
type Option func(*appOptions)

// WorkerPoolOption tunes worker-pool mode; pass WorkerPoolOptions to
// WithWorkerPool.
type WorkerPoolOption func(*appOptions)

// WithWorkerPool enables worker-pool mode: liveness heartbeats plus a sweeper
// that takes over the PENDING runs of dead executors (see the package-level
// worker-pool documentation). Every process in the fleet must enable it, and
// each needs a distinct executor identity — one is generated when neither
// Config.ExecutorID nor DBOS__VMID is set.
func WithWorkerPool(opts ...WorkerPoolOption) Option {
	return func(o *appOptions) {
		o.workerPool = true
		for _, wp := range opts {
			wp(o)
		}
	}
}

// WithHeartbeatInterval sets how often each process refreshes its liveness
// lease (default 10s). Keep it well below the stale threshold.
func WithHeartbeatInterval(d time.Duration) WorkerPoolOption {
	return func(o *appOptions) { o.heartbeatInterval = d }
}

// WithStaleThreshold sets how long a lease may go unrefreshed before its
// executor is considered dead and its PENDING runs are taken over (default
// 60s). Must be well above WithHeartbeatInterval plus worst-case scheduling
// jitter and GC pauses — too low and a merely-slow process is treated as dead
// and its live run is run a second time elsewhere. Avoid sub-second values in
// production.
func WithStaleThreshold(d time.Duration) WorkerPoolOption {
	return func(o *appOptions) { o.staleThreshold = d }
}

// WithSweepInterval sets how often each process runs its maintenance cycle
// (default 30s): the scan for dead executors to take over, plus retention and
// the stale-run warning, which run on the same cadence. It is a plain Option,
// not a WorkerPoolOption, because it governs all three — an app using only
// WithRetention or WithStaleRunWarning can still tune it.
//
// Fleet-wide the work is serialized by Postgres advisory locks, so this is
// per-process cadence, not global.
func WithSweepInterval(d time.Duration) Option {
	return func(o *appOptions) { o.sweepInterval = d }
}

// WithStaleRunWarning surfaces runs older than age that are still waiting or
// running, split by whether this executor's application version can recover them
// (see StaleRunInfo) — so stranded runs, which DBOS otherwise reports nowhere,
// become visible. It logs a warning with the counts; pass an optional hook to
// also receive them programmatically (invoked only when there is something to
// report). Usable with or without WithWorkerPool.
//
// Choose age above the longest a healthy run legitimately takes. Runs parked in
// a Delay stage stay PENDING for the whole pause, so an age below your longest
// Delay reports healthy runs as stale. (Debounced runs, which park in DELAYED,
// are already excluded.)
//
// DBOS v1 scopes the counts to this application and any unclaimed rows left by
// pre-v1 migrations. Runs owned by other applications in the same database are
// not included.
func WithStaleRunWarning(age time.Duration, hook ...func(StaleRunInfo)) Option {
	return func(o *appOptions) {
		o.staleRunWarn, o.staleRunWarnSet = age, true
		if len(hook) > 0 {
			o.staleRunHook = hook[0]
		}
	}
}

// WithRetention batch-deletes terminal runs that completed more than d ago,
// bounding the otherwise unbounded growth of workflow history (DBOS open source
// has no built-in retention). Each maintenance cycle deletes at most one batch,
// so a large backlog clears gradually over many cycles instead of in one long
// transaction; deletion holds its own advisory lock, never the sweeper's, so
// retention can never delay a takeover. Usable with or without WithWorkerPool.
//
// DBOS v1 scopes retention to this application and any unclaimed rows left by
// pre-v1 migrations. It does not delete runs owned by other applications that
// share the database.
func WithRetention(d time.Duration) Option {
	return func(o *appOptions) { o.retention, o.retentionSet = d, true }
}

// workerPool owns the liveness machinery for one App: the dedicated Postgres
// pool, the heartbeat writer, and the sweeper. It is nil unless worker-pool
// mode (or retention / stale-run warning) is enabled.
type workerPool struct {
	pool       *pgxpool.Pool
	logger     *slog.Logger
	dctx       dbos.Context
	appName    string
	executorID string
	version    string

	// nonce identifies this *process* within an executor ID. Two processes
	// sharing an ID (a DBOS__VMID set per deployment rather than per pod) would
	// otherwise share one lease invisibly, and either one's shutdown would
	// tombstone the other's liveness — handing a live process's runs to the
	// fleet. Each beat re-asserts this nonce and reports if it has been taken.
	nonce string

	enabled           bool // worker-pool takeover (heartbeat + sweeper) enabled
	heartbeatInterval time.Duration
	staleThreshold    time.Duration
	sweepInterval     time.Duration
	leasePruneAge     time.Duration
	retention         time.Duration
	staleRunWarn      time.Duration
	staleRunHook      func(StaleRunInfo)

	// tracker records this executor's executions (executions.go). It is set
	// exactly when enabled: the lease is what bounds a dead executor's open
	// execution rows.
	tracker *executionTracker

	// beat* controls the heartbeat goroutine; maint* controls the sweeper,
	// retention, and stale-run-warning goroutines. They are separate so
	// Close can stop maintenance first yet keep heartbeating while DBOS unwinds
	// (so this node's still-executing runs stay fresh and un-takeable).
	//
	// maintDctx is the DBOS-side half of maintCtx. The maintenance duties that
	// go through DBOS (retention's list and delete, the stale-run aggregate)
	// need a DBOS Context, and one derived from the app's would not observe
	// maintCancel at all — a query already in flight would then run to its own
	// opTimeout while Close waited on maintWG, burning the caller's SIGTERM
	// budget before DBOS shutdown even started. stopMaintenance cancels both.
	beatCtx         context.Context
	beatCancel      context.CancelFunc
	beatWG          sync.WaitGroup
	maintCtx        context.Context
	maintCancel     context.CancelFunc
	maintDctx       dbos.Context
	maintDctxCancel context.CancelFunc
	maintWG         sync.WaitGroup

	// closeOnce keeps a second Close from tombstoning against a closed pool.
	closeOnce sync.Once

	// notLatestWarned is whether the stale-run warning last found this
	// executor off the latest registered application version. Touched only by
	// the maintenance goroutine.
	notLatestWarned bool
}

// newWorkerPool opens duro's dedicated pool and ensures the heartbeat table.
// It reads the *resolved* executor ID and application version off the DBOS
// context, so env-var overrides and DBOS's defaults are already applied.
func newWorkerPool(ctx context.Context, cfg Config, dctx dbos.Context, o appOptions, logger *slog.Logger) (*workerPool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("duro: worker-pool: parsing database URL: %w", err)
	}
	// A small pool is plenty: heartbeats and sweeps are tiny, infrequent
	// queries. Sized so the heartbeat is never starved behind a slow sweep.
	pcfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("duro: worker-pool: opening pool: %w", err)
	}
	w := &workerPool{
		pool:              pool,
		logger:            logger,
		dctx:              dctx,
		appName:           cfg.Name,
		executorID:        dctx.GetExecutorID(),
		version:           dctx.GetApplicationVersion(),
		nonce:             randomHex(),
		enabled:           o.workerPool,
		heartbeatInterval: o.heartbeatInterval,
		staleThreshold:    o.staleThreshold,
		sweepInterval:     o.sweepInterval,
		retention:         o.retention,
		staleRunWarn:      o.staleRunWarn,
		staleRunHook:      o.staleRunHook,
		// Leases of long-gone executors are pruned once they are far past the
		// point of being useful evidence. Scaled off the stale threshold so the
		// pruning horizon always sits far beyond the takeover horizon.
		leasePruneAge: 100 * o.staleThreshold,
	}
	// The heartbeat table backs takeover only; retention / stale-run warning use
	// the pool (for the advisory lock) but never the lease.
	if w.enabled {
		if err := w.ensureTable(ctx); err != nil {
			pool.Close()
			return nil, err
		}
		tracker, err := newExecutionTracker(ctx, cfg.DatabaseURL, w.executorID, logger)
		if err != nil {
			pool.Close()
			return nil, err
		}
		w.tracker = tracker
	}
	return w, nil
}

// ensureTable creates duro's worker-pool tables and columns if they do not
// exist. The dbos schema already exists here — DBOS runs its migrations during
// NewContext, before duro's New returns.
//
// The DDL runs in one transaction under an advisory lock: two processes
// running CREATE TABLE IF NOT EXISTS at the same moment can still collide on
// the catalog's unique index and fail New, which is exactly what a fleet
// booting together (a deploy) does.
func (w *workerPool) ensureTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("duro: worker-pool: begin schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rolled back unless Commit succeeds first
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, schemaLockKey); err != nil {
		return fmt.Errorf("duro: worker-pool: schema lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+heartbeatTable+` (
		executor_id         TEXT PRIMARY KEY,
		app_name            TEXT NOT NULL,
		application_version TEXT NOT NULL,
		last_heartbeat      TIMESTAMPTZ NOT NULL,
		nonce               TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		return fmt.Errorf("duro: worker-pool: creating heartbeat table: %w", err)
	}
	// Tables created by an earlier duro version predate the nonce column.
	if _, err := tx.Exec(ctx, `ALTER TABLE `+heartbeatTable+
		` ADD COLUMN IF NOT EXISTS nonce TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("duro: worker-pool: adding heartbeat nonce column: %w", err)
	}
	// Each executor publishes the stale threshold it heartbeats against, so a
	// reader judging its liveness (Unsettled, possibly from a Client that
	// knows nothing of the fleet's cadence) uses the executor's own. Rows an
	// earlier duro version keeps writing get the default threshold.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS stale_after_ms BIGINT NOT NULL DEFAULT %d`,
		heartbeatTable, defaultStaleThreshold.Milliseconds())); err != nil {
		return fmt.Errorf("duro: worker-pool: adding heartbeat stale threshold column: %w", err)
	}
	if _, err := tx.Exec(ctx, createExecutionsSQL); err != nil {
		return fmt.Errorf("duro: worker-pool: creating executions table: %w", err)
	}
	if _, err := tx.Exec(ctx, createExecutionsIndexSQL); err != nil {
		return fmt.Errorf("duro: worker-pool: creating executions index: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("duro: worker-pool: commit schema transaction: %w", err)
	}
	return nil
}

// schemaLockKey names the advisory lock ensureTable holds. It is shared by
// every duro app on the database, since the tables are too.
const schemaLockKey = "duro:schema"

// claimLease writes this process's lease unconditionally, taking ownership of
// the executor ID. It runs once, before DBOS launches: a restart legitimately
// reclaims its own ID, and there is nothing yet to protect.
func (w *workerPool) claimLease(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_, err := w.pool.Exec(ctx, `INSERT INTO `+heartbeatTable+`
		(executor_id, app_name, application_version, last_heartbeat, nonce, stale_after_ms)
		VALUES ($1, $2, $3, now(), $4, $5)
		ON CONFLICT (executor_id) DO UPDATE SET
			app_name            = EXCLUDED.app_name,
			application_version = EXCLUDED.application_version,
			last_heartbeat      = now(),
			nonce               = EXCLUDED.nonce,
			stale_after_ms      = EXCLUDED.stale_after_ms`,
		w.executorID, w.appName, w.version, w.nonce, w.staleThreshold.Milliseconds())
	return err
}

// errLeaseStolen reports that another live process is heartbeating under this
// executor ID — a duplicate identity, which makes the liveness signal (and every
// takeover decision resting on it) meaningless for both processes.
var errLeaseStolen = errors.New("duro: worker-pool: executor ID is in use by another process")

// beat refreshes this process's lease, stamping last_heartbeat with the database
// clock so staleness comparisons are immune to cross-process clock skew. The
// nonce predicate makes the write conditional on still owning the lease: if
// another process has claimed the same executor ID, no row is updated and the
// collision is reported instead of silently corrupting both processes' liveness.
func (w *workerPool) beat(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	tag, err := w.pool.Exec(ctx, `INSERT INTO `+heartbeatTable+`
		(executor_id, app_name, application_version, last_heartbeat, nonce, stale_after_ms)
		VALUES ($1, $2, $3, now(), $4, $5)
		ON CONFLICT (executor_id) DO UPDATE SET
			app_name            = EXCLUDED.app_name,
			application_version = EXCLUDED.application_version,
			last_heartbeat      = now(),
			stale_after_ms      = EXCLUDED.stale_after_ms
		WHERE `+heartbeatTable+`.nonce = EXCLUDED.nonce`,
		w.executorID, w.appName, w.version, w.nonce, w.staleThreshold.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errLeaseStolen
	}
	return nil
}

// tombstone marks this executor's lease as long-dead (last_heartbeat at the
// epoch), so a graceful shutdown's interrupted runs are eligible for takeover on
// the next survivor sweep with no stale wait. An absent row means "unknown —
// do not touch"; a tombstoned row means "known-dead — take over now".
//
// The nonce predicate keeps a departing process from tombstoning a lease another
// process has since claimed — which would declare a live executor dead.
func (w *workerPool) tombstone(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, tombstoneTimeout)
	defer cancel()
	_, err := w.pool.Exec(ctx, `UPDATE `+heartbeatTable+`
		SET last_heartbeat = to_timestamp(0) WHERE executor_id = $1 AND nonce = $2`,
		w.executorID, w.nonce)
	return err
}

// firstBeat claims this process's lease synchronously, before queue runners
// start — an executor must never be dequeuing while observably dead.
func (w *workerPool) firstBeat(ctx context.Context) error {
	if err := w.claimLease(ctx); err != nil {
		return fmt.Errorf("duro: worker-pool: initial heartbeat: %w", err)
	}
	if w.tracker != nil {
		ctx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		if _, err := w.tracker.pool.Exec(ctx, closeOrphanedSQL, w.executorID); err != nil {
			return fmt.Errorf("duro: worker-pool: closing a previous process's executions: %w", err)
		}
	}
	return nil
}

// start launches the background goroutines after DBOS has launched. The
// heartbeat ticker runs under beatCtx; the sweeper and other maintenance run
// under maintCtx.
func (w *workerPool) start() {
	w.beatCtx, w.beatCancel = context.WithCancel(context.Background())
	w.initMaintContexts()

	if w.enabled {
		w.beatWG.Add(1)
		go w.heartbeatLoop()
		w.maintWG.Add(1)
		go w.executionLoop()
	}
	w.startMaintenance()
}

// initMaintContexts derives both halves of the maintenance scope. Every
// maintenance database call must hang off one of them — the pgx ones off
// maintCtx, the DBOS-routed ones off maintDctx — so stopMaintenance aborts
// whatever is in flight instead of waiting it out.
func (w *workerPool) initMaintContexts() {
	w.maintCtx, w.maintCancel = context.WithCancel(context.Background())
	w.maintDctx, w.maintDctxCancel = dbos.WithCancel(w.dctx)
}

func (w *workerPool) heartbeatLoop() {
	defer w.beatWG.Done()
	t := time.NewTicker(w.heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-w.beatCtx.Done():
			return
		case <-t.C:
			err := w.beat(w.beatCtx)
			switch {
			case err == nil || w.beatCtx.Err() != nil:
			case errors.Is(err, errLeaseStolen):
				// Two processes on one executor ID: neither's liveness means
				// anything, and either one's shutdown can hand the other's live
				// runs to the fleet. Nothing duro can safely do about it from
				// here, so say so loudly, every beat, until it is fixed.
				w.logger.Error("duro: worker-pool: another process is heartbeating under this executor ID — give each process a unique Config.ExecutorID (or DBOS__VMID); until then this fleet's takeover decisions are unsafe",
					"executor_id", w.executorID, "app", w.appName)
			default:
				w.logger.Warn("duro: worker-pool: heartbeat failed", "executor_id", w.executorID, "error", err)
			}
		}
	}
}

// executionLoop drives the execution tracker's poll (cancellation and failed
// closes) on the heartbeat cadence, so a cancelled run's stage is interrupted
// within about one heartbeat interval. It runs in the maintenance scope, on
// its own goroutine and the tracker's own pool: it must neither wait behind a
// sweep nor delay a beat.
func (w *workerPool) executionLoop() {
	defer w.maintWG.Done()
	t := time.NewTicker(w.heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-w.maintCtx.Done():
			return
		case <-t.C:
			w.tracker.poll(w.maintCtx)
		}
	}
}

// startMaintenance launches the maintenance goroutine (takeover sweep, stale-run
// warning, retention) when any of them is configured.
func (w *workerPool) startMaintenance() {
	if !w.enabled && w.retention == 0 && w.staleRunWarn == 0 {
		return
	}
	w.maintWG.Add(1)
	go w.maintenanceLoop()
}

// maintenanceLoop runs the periodic worker-pool duties at the sweep cadence
// until maintCtx is cancelled (which Close does before DBOS shutdown).
func (w *workerPool) maintenanceLoop() {
	defer w.maintWG.Done()
	t := time.NewTicker(w.sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-w.maintCtx.Done():
			return
		case <-t.C:
			if w.enabled {
				w.runSweep()
				w.runPruneLeases()
			}
			if w.staleRunWarn > 0 {
				w.runStaleRunWarning()
			}
			if w.retention > 0 {
				w.runRetention()
			}
		}
	}
}

func (w *workerPool) runSweep() {
	switch n, err := w.sweepOnce(w.maintCtx); {
	case err != nil && w.maintCtx.Err() == nil:
		w.logger.Warn("duro: worker-pool: sweep failed", "error", err)
	case n > 0:
		w.logger.Info("duro: worker-pool: took over stranded runs", "count", n, "sweeper", w.executorID)
	}
}

// lockKey names the per-app sweeper advisory lock so unrelated duro apps sharing
// one Postgres neither serialize against nor collide with each other.
func (w *workerPool) lockKey() string { return "duro:sweeper:" + w.appName }

// retentionLockKey names a separate lock for retention. Retention must never
// share the sweeper's lock: a slow delete would make every other node's sweep
// skip its cycle, stalling takeover — the safety-critical path — fleet-wide
// behind mere housekeeping.
func (w *workerPool) retentionLockKey() string { return "duro:retention:" + w.appName }

// withLock runs fn while holding a transaction-scoped advisory lock, reporting
// whether the lock was acquired. The transaction exists only to own the lock, so
// fn's own work is not rolled back with it.
func (w *workerPool) withLock(ctx context.Context, key string, fn func(pgx.Tx) error) (bool, error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("duro: worker-pool: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rolled back unless Commit succeeds first; either way releases the lock

	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`, key).Scan(&locked); err != nil {
		return false, fmt.Errorf("duro: worker-pool: advisory lock: %w", err)
	}
	if !locked {
		return false, nil // another process holds it; skip this cycle
	}
	if err := fn(tx); err != nil {
		return true, err
	}
	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("duro: worker-pool: commit: %w", err)
	}
	return true, nil
}

// sweepOnce takes over dead executors' runs once, serialized fleet-wide by a
// best-effort transaction-scoped advisory lock. Correctness does not depend on
// the lock — the takeover UPDATE is idempotent and ownership-checked — so a
// process that cannot acquire it simply skips the cycle.
func (w *workerPool) sweepOnce(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	var adopted int
	_, err := w.withLock(ctx, w.lockKey(), func(tx pgx.Tx) error {
		ids, err := w.takeover(ctx, tx)
		if err != nil {
			return err
		}
		adopted = len(ids)
		return nil
	})
	return adopted, err
}

// takeoverSQL re-enqueues the PENDING runs of dead executors on this app's
// application version. It mirrors DBOS's own resume (system_database.go
// resumeWorkflows) — largely the same column writes — but adds the executor_id,
// application_version and application_name predicates that make it safe:
//
//   - The ownership check runs atomically under the rows' locks, so a run a live
//     executor has already re-claimed (its executor_id no longer among the dead
//     set) is never yanked back to ENQUEUED — closing the list-then-resume race
//     that raw dbos.ResumeWorkflows would open.
//   - Application ownership is equally atomic. V1-owned rows must match this
//     app; migrated v0 rows are unclaimed (NULL), so takeover claims them for
//     the dead executor's known app before they return to a shared queue.
//   - Gating on liveness (heartbeat staleness), not dequeue time, covers
//     directly-started pipelines, whose started_at_epoch_ms is NULL.
//
// The queue is where it follows DBOS's *recovery* rather than its resume.
// Recovery (recovery.go's clearQueueAssignment) leaves queue_name alone, so a
// recovered run goes back on its own queue; resume rewrites it to the internal
// queue, which is why resume grew a WithResumeQueue option. Sending an adopted
// run to the internal queue would exempt it from its queue's concurrency and
// rate limits and stop it counting against them — enough for a
// WithConcurrency(1) queue to have two runs executing at once — and would drop
// a partitioned queue's run out of its partition. So the queue is preserved
// when there is one, and the internal queue is the fallback for the runs that
// have none (a directly-started pipeline has no queue to return to, and
// nothing but a queue runner can restart it from here).
//
// NULLIF keeps compatibility with v0 rows, where DBOS wrote the empty string
// for a directly started run. V1 writes NULL. COALESCE handles both and avoids
// an empty queue name that no runner polls.
//
// Preserving the queue does assume some live executor on this application
// version polls it. That holds by construction for pipelines: Register
// registers every queue its pipeline references, on every process.
//
// It deliberately does NOT reset recovery_attempts, where it parts company with
// DBOS's resume. Resume is a deliberate operator action; takeover is an
// automatic loop, and zeroing the counter on every adoption would disable DBOS's
// poison-run circuit breaker: a run that kills whichever process executes it
// (an OOM, a step that panics the process) would be adopted, kill the next
// worker, be adopted again — walking the fleet down one node at a time, forever,
// with no terminal state. Leaving the counter alone lets DBOS increment it on
// each start and eventually park the run in MAX_RECOVERY_ATTEMPTS_EXCEEDED,
// where this UPDATE (PENDING-only) will never pick it up again.
//
// It is coupled to DBOS's internal workflow_status schema (pinned to the dbos
// module version); a column rename fails this UPDATE loudly rather than
// corrupting anything silently.
const takeoverSQL = `UPDATE dbos.workflow_status
	SET status = $1, queue_name = COALESCE(NULLIF(queue_name, ''), $2),
	    workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
	    started_at_epoch_ms = NULL, updated_at = $3, completed_at = NULL,
	    application_name = COALESCE(application_name, $6)
	WHERE status = $4
	  AND application_version = $5
	  AND (application_name = $6 OR application_name IS NULL)
	  AND executor_id IN (
	      SELECT executor_id FROM ` + heartbeatTable + `
	       WHERE app_name = $6 AND last_heartbeat < now() - make_interval(secs => $7)
	  )
	RETURNING workflow_uuid`

// takeover runs the takeover UPDATE and returns the IDs of the runs it adopted.
func (w *workerPool) takeover(ctx context.Context, q querier) ([]string, error) {
	rows, err := q.Query(ctx, takeoverSQL,
		string(dbos.WorkflowStatusEnqueued),
		internalQueueName,
		time.Now().UnixMilli(),
		string(dbos.WorkflowStatusPending),
		w.version,
		w.appName,
		w.staleThreshold.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("duro: worker-pool: takeover update: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("duro: worker-pool: scanning taken-over id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duro: worker-pool: reading taken-over ids: %w", err)
	}
	return ids, nil
}

// pruneLeasesSQL deletes leases of executors that are long gone. Without it the
// table grows without bound on exactly the infrastructure worker-pool mode
// targets, where every process start mints a fresh identity, and every sweep
// scans the accumulated rows.
//
// The NOT EXISTS guard is load-bearing: an absent lease means "unknown executor,
// do not touch its runs", so pruning a lease whose executor still owns an
// adoptable run would make that run permanently unadoptable.
//
// PENDING is the only status that needs guarding, and matching it exactly is
// what keeps this cheap. Correctness: takeoverSQL adopts PENDING rows only, so a
// lease is consulted for no other status — an ENQUEUED or DELAYED run is
// dequeued by queue name, status and version, and the dequeue stamps the live
// dequeuer's executor_id, so its previous owner's lease is irrelevant to it.
// Cost: DBOS keeps no index on executor_id, and Postgres can only use a partial
// index when the query's predicate implies the index's, so a wider status set
// (IN (PENDING, ENQUEUED, DELAYED)) implies none of them and degrades to a full
// scan of all history. `status = 'PENDING'` implies idx_workflow_status_pending,
// bounding the lookup by in-flight work instead.
const pruneLeasesSQL = `DELETE FROM ` + heartbeatTable + ` h
	WHERE h.app_name = $1
	  AND h.last_heartbeat < now() - make_interval(secs => $2)
	  AND NOT EXISTS (
		      SELECT 1 FROM dbos.workflow_status ws
		       WHERE ws.executor_id = h.executor_id
		         AND ws.status = $3
		         AND (ws.application_name = h.app_name OR ws.application_name IS NULL)
		  )`

// pruneExecutionsSQL deletes the execution rows of executors whose leases are
// long dead, on the same horizon as pruneLeasesSQL. It does not share that
// statement's PENDING guard: a dead executor's rows count for nothing in
// Unsettled whether or not its lease survives, so they can always go.
const pruneExecutionsSQL = `DELETE FROM ` + executionsTable + ` e
	USING ` + heartbeatTable + ` h
	WHERE e.executor_id = h.executor_id
	  AND h.app_name = $1
	  AND h.last_heartbeat < now() - make_interval(secs => $2)`

// pruneLeases removes long-dead leases and their execution rows; see
// pruneLeasesSQL and pruneExecutionsSQL. The rows go first: a lease deleted
// before them would leave rows nothing ever matches again.
func (w *workerPool) pruneLeases(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if _, err := w.pool.Exec(ctx, pruneExecutionsSQL, w.appName, w.leasePruneAge.Seconds()); err != nil {
		return 0, fmt.Errorf("duro: worker-pool: pruning execution rows: %w", err)
	}
	tag, err := w.pool.Exec(ctx, pruneLeasesSQL,
		w.appName,
		w.leasePruneAge.Seconds(),
		string(dbos.WorkflowStatusPending),
	)
	if err != nil {
		return 0, fmt.Errorf("duro: worker-pool: pruning heartbeats: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (w *workerPool) runPruneLeases() {
	n, err := w.pruneLeases(w.maintCtx)
	if err != nil {
		if w.maintCtx.Err() == nil {
			w.logger.Warn("duro: worker-pool: pruning heartbeats failed", "error", err)
		}
		return
	}
	if n > 0 {
		w.logger.Debug("duro: worker-pool: pruned long-dead executor leases", "count", n)
	}
}

// --- R4: stale-run warning -------------------------------------------------

// runStaleRunWarning counts non-terminal runs older than the configured age,
// split by whether they can be recovered on this executor's application
// version, and surfaces them — stranded runs otherwise emit no signal at all.
func (w *workerPool) runStaleRunWarning() {
	info, latest, err := w.checkStaleRuns()
	if err != nil {
		if w.maintCtx.Err() == nil {
			w.logger.Warn("duro: worker-pool: stale-run check failed", "error", err)
		}
		return
	}
	w.noteLatestVersion(latest)
	if info.SameVersion == 0 && info.OtherVersion == 0 {
		return
	}
	w.logger.Warn("duro: worker-pool: stale non-terminal runs detected — some may be stranded",
		"same_version", info.SameVersion, "other_version", info.OtherVersion, "older_than", w.staleRunWarn)
	if w.staleRunHook != nil {
		w.staleRunHook(info)
	}
}

// noteLatestVersion warns when this executor stops being on the latest
// registered application version, and says so when it is again. It logs on the
// change rather than every cycle: the condition lasts until someone acts.
func (w *workerPool) noteLatestVersion(latest string) {
	notLatest := latest != w.version
	if notLatest == w.notLatestWarned {
		return
	}
	w.notLatestWarned = notLatest
	if notLatest {
		w.logger.Warn(notLatestVersionWarning, "application_version", w.version, "latest_version", latest)
		return
	}
	w.logger.Info("duro: this executor is on the latest registered application version again", "application_version", w.version)
}

// checkStaleRuns tallies runs created before now-age that are still waiting to
// run or running, splitting the ones this application version can run (in
// limbo, recoverable here) from the rest (not recoverable until an executor on
// the right version runs). It also returns the latest registered application
// version, which decides where runs with no recorded version belong.
//
// It aggregates in the database rather than listing rows, so the counts are
// exact however large the backlog — a warning that silently capped its own
// number would defeat the point.
//
// DELAYED runs are deliberately excluded: a debounced pipeline parks there by
// design, so counting them would report healthy work as stranded.
func (w *workerPool) checkStaleRuns() (StaleRunInfo, string, error) {
	ctx, cancel := dbos.WithTimeout(w.maintDctx, opTimeout)
	defer cancel()

	rows, err := dbos.GetWorkflowAggregates(ctx, dbos.GetWorkflowAggregatesInput{
		GroupByApplicationVersion: true,
		SelectCount:               true,
		Status: []dbos.WorkflowStatusType{
			dbos.WorkflowStatusPending,
			dbos.WorkflowStatusEnqueued,
		},
		EndTime: time.Now().Add(-w.staleRunWarn), // created_at ≤ cutoff
	})
	if err != nil {
		return StaleRunInfo{}, "", fmt.Errorf("duro: worker-pool: counting stale runs: %w", err)
	}
	latest, err := latestApplicationVersion(ctx, w.version)
	if err != nil {
		return StaleRunInfo{}, "", fmt.Errorf("duro: worker-pool: reading the latest application version: %w", err)
	}
	return splitStaleRuns(rows, w.version, latest == w.version), latest, nil
}

// splitStaleRuns sums per-version run counts into the runs an executor on
// version can run and the ones it cannot. A run with no recorded version
// belongs to whichever version is latest: that is the only one DBOS lets
// dequeue it.
func splitStaleRuns(rows []dbos.WorkflowAggregateRow, version string, versionIsLatest bool) StaleRunInfo {
	var info StaleRunInfo
	for _, r := range rows {
		if r.Count == nil {
			continue
		}
		recorded := r.Group["application_version"]
		if (recorded == nil && versionIsLatest) || (recorded != nil && *recorded == version) {
			info.SameVersion += int(*r.Count)
		} else {
			info.OtherVersion += int(*r.Count)
		}
	}
	return info
}

// notLatestVersionWarning is logged at Launch and by the stale-run warning.
const notLatestVersionWarning = "duro: this executor is not on the latest registered application version: DBOS hands runs enqueued with no version — a Client's default — to the latest version only, so it will never dequeue them"

// latestApplicationVersion returns the application version DBOS treats as
// latest for this application — the only one allowed to dequeue runs recorded
// with no version. With no version registered at all DBOS treats the asking
// executor as latest; so does this.
func latestApplicationVersion(ctx dbos.Context, version string) (string, error) {
	latest, err := dbos.GetLatestApplicationVersion(ctx)
	switch {
	case errors.Is(err, dbos.ErrNoApplicationVersions):
		return version, nil
	case err != nil:
		return "", err
	}
	return latest.Name, nil
}

// --- R5: retention ---------------------------------------------------------

// retentionBatchSize bounds how many terminal runs one retention cycle deletes.
// A backlog larger than this clears over subsequent cycles: bounded work per
// cycle keeps the maintenance goroutine — which also drives takeover sweeps on
// this process — responsive no matter how much history has accumulated.
const retentionBatchSize = 100

// runRetention deletes one batch of expired terminal runs.
func (w *workerPool) runRetention() {
	n, err := w.sweepRetention()
	if err != nil {
		if w.maintCtx.Err() == nil {
			w.logger.Warn("duro: worker-pool: retention failed", "error", err)
		}
		return
	}
	if n > 0 {
		w.logger.Info("duro: worker-pool: deleted expired runs", "count", n, "older_than", w.retention)
	}
}

// sweepRetention deletes at most one batch of terminal runs that completed
// before the retention cutoff, under retention's own advisory lock (never the
// sweeper's — housekeeping must not delay takeover). Every database call is
// deadline-bounded so a slow delete cannot wedge the maintenance goroutine.
func (w *workerPool) sweepRetention() (int, error) {
	ctx, cancel := context.WithTimeout(w.maintCtx, opTimeout)
	defer cancel()

	deleted := 0
	_, err := w.withLock(ctx, w.retentionLockKey(), func(pgx.Tx) error {
		dctx, dcancel := dbos.WithTimeout(w.maintDctx, opTimeout)
		defer dcancel()

		terminal, err := dbos.ListWorkflows(dctx,
			dbos.WithFilterStatus([]dbos.WorkflowStatusType{
				dbos.WorkflowStatusSuccess,
				dbos.WorkflowStatusError,
				dbos.WorkflowStatusCancelled,
				dbos.WorkflowStatusMaxRecoveryAttemptsExceeded,
			}...),
			dbos.WithFilterCompletedBefore(time.Now().Add(-w.retention)),
			dbos.WithFilterLoadInput(false),
			dbos.WithFilterLoadOutput(false),
			dbos.WithFilterLimit(retentionBatchSize),
		)
		if err != nil {
			return fmt.Errorf("duro: worker-pool: listing expired runs: %w", err)
		}
		if len(terminal) == 0 {
			return nil
		}
		ids := make([]string, len(terminal))
		for i, r := range terminal {
			ids[i] = r.ID
		}
		if err := dbos.DeleteWorkflows(dctx, ids); err != nil {
			return fmt.Errorf("duro: worker-pool: deleting expired runs: %w", err)
		}
		deleted = len(ids)
		if w.enabled {
			// The runs are gone, so nothing asks about their executions. A
			// failure here leaves rows that go with their executor's lease.
			if _, err := w.pool.Exec(ctx, `DELETE FROM `+executionsTable+` WHERE workflow_uuid = ANY($1)`, ids); err != nil {
				return fmt.Errorf("duro: worker-pool: deleting expired runs' execution rows: %w", err)
			}
		}
		return nil
	})
	return deleted, err
}

// stopMaintenance stops the sweeper and other maintenance goroutines, leaving
// the heartbeat running (Close keeps beating while DBOS unwinds).
//
// It cancels both halves of the maintenance scope, so a database call already
// in flight is aborted rather than waited out: Close proceeds to DBOS shutdown
// promptly instead of holding for an operation timeout first.
func (w *workerPool) stopMaintenance() {
	if w.maintCancel != nil {
		w.maintCancel()
	}
	if w.maintDctxCancel != nil {
		w.maintDctxCancel()
	}
	w.maintWG.Wait()
}

// stopBeat stops the heartbeat goroutine.
func (w *workerPool) stopBeat() {
	if w.beatCancel != nil {
		w.beatCancel()
	}
	w.beatWG.Wait()
}

func (w *workerPool) close() {
	w.pool.Close()
	if w.tracker != nil {
		w.tracker.close()
	}
}

// generateExecutorID mints a process-unique executor identity for worker-pool
// mode. A hostname prefix keeps it recognizable in logs and the heartbeat
// table; the random suffix guarantees uniqueness across processes on one host.
func generateExecutorID() string {
	id := randomHex()
	if host, err := os.Hostname(); err == nil && host != "" {
		return host + "-" + id
	}
	return "duro-" + id
}

// randomHex returns 8 random bytes as hex, used for executor IDs and lease
// nonces.
func randomHex() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails in practice
	return hex.EncodeToString(b[:])
}
