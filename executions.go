package duro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Execution tracking answers the question DBOS's schema cannot: is any code of
// this run still executing? A run's state says what it will do next, not what
// its executor is doing now. Cancellation writes CANCELLED to the run's row
// while the stage in flight keeps running on its executor, and nothing DBOS
// records says when that stage returns.
//
// In worker-pool mode, every execution of a registered pipeline (and of duro's
// cancellation watcher) writes a row to executionsTable before its first stage
// and stamps ended_at on it after its last stage has returned. Each row names
// the executor that wrote it, so a row left open by a process that died is
// discounted as soon as that executor's lease goes stale: the same liveness
// signal takeover rests on. Unsettled combines the two into "is this run in a
// final state, and is none of its code still running?"
//
// The tracker also makes cancellation stop work. It polls the state of the
// runs it is executing, and when one turns CANCELLED it cancels that run's
// context, so the stage in flight sees ctx.Done() within a heartbeat interval
// instead of finishing its whole retry budget. See executeTracked.

// executionsTable is duro's execution record, next to the heartbeat table in
// the dbos schema.
const executionsTable = "dbos.duro_run_executions"

// errRunCancelled is the cause a tracked run's context carries once the
// tracker has seen the run cancelled.
var errRunCancelled = errors.New("duro: run was cancelled")

// trackerPoolConns sizes the tracker's own pool. The tracker writes on every
// run start and end, so it must never borrow the heartbeat's pool: a burst of
// run starts queued behind four connections would delay beats, and a late
// beat hands this executor's live runs to the fleet.
const trackerPoolConns = 4

// Schema. The PRIMARY KEY leads with executor_id for the tracker's own
// writes; Unsettled looks rows up by run, hence the second index. A row stays
// after its execution ends (ended_at set): its presence is what tells
// Unsettled that this executor ran the run under tracking. Rows are deleted
// with their run by retention, or with their executor's lease once it is
// pruned.
const createExecutionsSQL = `CREATE TABLE IF NOT EXISTS ` + executionsTable + ` (
		executor_id   TEXT NOT NULL,
		workflow_uuid TEXT NOT NULL,
		token         TEXT NOT NULL,
		started_at    TIMESTAMPTZ NOT NULL,
		ended_at      TIMESTAMPTZ,
		PRIMARY KEY (executor_id, workflow_uuid)
	)`

const createExecutionsIndexSQL = `CREATE INDEX IF NOT EXISTS duro_run_executions_workflow_uuid_idx
	ON ` + executionsTable + ` (workflow_uuid)`

// startExecutionSQL opens this executor's row for a run. A run executes on one
// executor at most once at a time (DBOS refuses a second concurrent start on
// the same executor), so a conflicting row is an earlier, finished execution
// here, and is reopened under the new token.
const startExecutionSQL = `INSERT INTO ` + executionsTable + `
		(executor_id, workflow_uuid, token, started_at, ended_at)
		VALUES ($1, $2, $3, now(), NULL)
	ON CONFLICT (executor_id, workflow_uuid) DO UPDATE SET
		token = EXCLUDED.token, started_at = EXCLUDED.started_at, ended_at = NULL`

// endExecutionSQL closes a row. The token keeps a late or retried close from
// closing a newer execution of the same run on this executor.
const endExecutionSQL = `UPDATE ` + executionsTable + `
	SET ended_at = now()
	WHERE executor_id = $1 AND workflow_uuid = $2 AND token = $3 AND ended_at IS NULL`

// endExecutionsSQL is endExecutionSQL for the closes the tracker retries.
const endExecutionsSQL = `UPDATE ` + executionsTable + ` e
	SET ended_at = now()
	FROM unnest($2::text[], $3::text[]) AS u(workflow_uuid, token)
	WHERE e.executor_id = $1 AND e.workflow_uuid = u.workflow_uuid
	  AND e.token = u.token AND e.ended_at IS NULL`

// closeOrphanedSQL closes every row this executor ID still has open. It runs
// when a process claims the ID, before it executes anything: a restart under
// a reused ID (Config.ExecutorID, DBOS__VMID) inherits a crashed
// predecessor's rows, which the newly live lease would otherwise keep
// counting forever.
const closeOrphanedSQL = `UPDATE ` + executionsTable + `
	SET ended_at = now()
	WHERE executor_id = $1 AND ended_at IS NULL`

// cancelledRunsSQL finds which of the runs this executor is executing have
// been cancelled.
const cancelledRunsSQL = `SELECT workflow_uuid FROM dbos.workflow_status
	WHERE workflow_uuid = ANY($1) AND status = $2`

// executionTracker records this executor's executions and propagates
// cancellation into them. It exists only in worker-pool mode, whose lease is
// what bounds a dead executor's open rows.
type executionTracker struct {
	pool       *pgxpool.Pool
	executorID string
	logger     *slog.Logger

	mu sync.Mutex
	// running holds the executions in progress here, by workflow ID.
	running map[string]*trackedExecution
	// unended holds executions that finished but whose close failed to
	// write, by workflow ID. The poll retries them; until one lands, the run
	// reads as executing, which is the safe direction.
	unended map[string]string
}

type trackedExecution struct {
	token  string
	cancel context.CancelCauseFunc
}

func newExecutionTracker(ctx context.Context, databaseURL, executorID string, logger *slog.Logger) (*executionTracker, error) {
	pcfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("duro: execution tracking: parsing database URL: %w", err)
	}
	pcfg.MaxConns = trackerPoolConns
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("duro: execution tracking: opening pool: %w", err)
	}
	return &executionTracker{
		pool:       pool,
		executorID: executorID,
		logger:     logger,
		running:    make(map[string]*trackedExecution),
		unended:    make(map[string]string),
	}, nil
}

func (t *executionTracker) close() { t.pool.Close() }

// executeTracked runs a workflow body as a tracked execution: the execution
// row is written before fn starts, fn runs on a context the tracker cancels if
// the run is cancelled, and the row is closed after fn returns. Without a
// tracker (worker-pool mode off) it runs fn unchanged.
//
// fn's context is a child of the workflow's, so a stage interrupted by
// cancellation returns the context's error through DBOS's step machinery,
// which records it as the stage's outcome: DBOS itself skips that record only
// when its own workflow context is cancelled, and that context is not
// reachable from here. The run is already CANCELLED, which Resume refuses
// once it has started (ErrRunInFlight), so the recorded error is visible in
// Steps and duro never replays it. DBOS's own resume paths
// (dbos.ResumeWorkflow, its admin server, Conductor) and a ForkFromStage from
// a later stage would replay it as that stage's failure.
func executeTracked[R any](ctx Context, fn func(Context) (R, error)) (R, error) {
	t := trackerFromContext(ctx)
	if t == nil {
		return fn(ctx)
	}
	runCtx, end, err := t.begin(ctx)
	if err != nil {
		var zero R
		return zero, err
	}
	defer end()
	return fn(runCtx)
}

// begin records the start of an execution of the workflow ctx belongs to and
// returns the context the body must run on, plus the func that ends it.
//
// The start write is retried until it lands, like DBOS's own writes: running
// the body without it would leave a stage executing that Unsettled cannot see,
// and failing the run over a transient database error would turn a blip into
// an ERROR outcome. It gives up only when ctx ends (shutdown or the run's
// deadline), returning the context's error, which DBOS treats as an
// interruption rather than a failure.
func (t *executionTracker) begin(ctx Context) (Context, func(), error) {
	id, err := dbos.GetWorkflowID(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("duro: execution tracking: %w", err)
	}
	token := randomHex()
	if err := t.recordStart(ctx, id, token); err != nil {
		// The last attempt may have committed before its context ended:
		// queue the close, which is a no-op when no row was written.
		t.mu.Lock()
		t.unended[id] = token
		t.mu.Unlock()
		return nil, nil, err
	}
	runCtx, cancel := dbos.WithCancelCause(ctx)
	t.mu.Lock()
	t.running[id] = &trackedExecution{token: token, cancel: cancel}
	delete(t.unended, id) // the start just reopened the row under a new token
	t.mu.Unlock()
	return runCtx, func() { t.end(id, token, cancel) }, nil
}

const (
	trackerRetryBase = 100 * time.Millisecond
	trackerRetryMax  = 5 * time.Second
)

func (t *executionTracker) recordStart(ctx context.Context, id, token string) error {
	delay := trackerRetryBase
	for {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		_, err := t.pool.Exec(opCtx, startExecutionSQL, t.executorID, id, token)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("duro: execution tracking: recording the start of %s: %w", id, ctx.Err())
		}
		t.logger.Warn("duro: execution tracking: recording a run's start failed; retrying", "workflow_id", id, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("duro: execution tracking: recording the start of %s: %w", id, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(2*delay, trackerRetryMax)
	}
}

// end closes the execution. A close that fails is queued for the poll to
// retry, unless the run has since started again here, which reopened the row.
func (t *executionTracker) end(id, token string, cancel context.CancelCauseFunc) {
	t.mu.Lock()
	if e := t.running[id]; e != nil && e.token == token {
		delete(t.running, id)
	}
	t.mu.Unlock()
	cancel(nil)

	ctx, done := context.WithTimeout(context.Background(), opTimeout)
	defer done()
	if _, err := t.pool.Exec(ctx, endExecutionSQL, t.executorID, id, token); err != nil {
		t.logger.Warn("duro: execution tracking: recording a run's end failed; it reads as executing until a retry lands", "workflow_id", id, "error", err)
		t.mu.Lock()
		if _, again := t.running[id]; !again {
			t.unended[id] = token
		}
		t.mu.Unlock()
	}
}

// poll cancels the running executions whose runs were cancelled and retries
// failed closes. The worker-pool maintenance scope drives it on the heartbeat
// cadence.
func (t *executionTracker) poll(ctx context.Context) {
	if err := t.cancelObserved(ctx); err != nil && ctx.Err() == nil {
		t.logger.Warn("duro: execution tracking: checking running runs for cancellation failed", "error", err)
	}
	if err := t.retryEnds(ctx); err != nil && ctx.Err() == nil {
		t.logger.Warn("duro: execution tracking: retrying run ends failed", "error", err)
	}
}

func (t *executionTracker) cancelObserved(ctx context.Context) error {
	t.mu.Lock()
	tokens := make(map[string]string, len(t.running))
	for id, e := range t.running {
		tokens[id] = e.token
	}
	t.mu.Unlock()
	if len(tokens) == 0 {
		return nil
	}
	ids := make([]string, 0, len(tokens))
	for id := range tokens {
		ids = append(ids, id)
	}

	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	rows, err := t.pool.Query(ctx, cancelledRunsSQL, ids, string(dbos.WorkflowStatusCancelled))
	if err != nil {
		return err
	}
	cancelled, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range cancelled {
		// Only the execution the poll looked at: a newer one of the same
		// run started after a resume and is not cancelled.
		if e := t.running[id]; e != nil && e.token == tokens[id] {
			e.cancel(errRunCancelled)
		}
	}
	return nil
}

func (t *executionTracker) retryEnds(ctx context.Context) error {
	t.mu.Lock()
	var ids, tokens []string
	for id, token := range t.unended {
		ids = append(ids, id)
		tokens = append(tokens, token)
	}
	t.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if _, err := t.pool.Exec(ctx, endExecutionsSQL, t.executorID, ids, tokens); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, id := range ids {
		if t.unended[id] == tokens[i] {
			delete(t.unended, id)
		}
	}
	return nil
}

func trackerFromContext(ctx Context) *executionTracker {
	if runtime := applicationRuntimeFromContext(ctx); runtime != nil {
		return runtime.tracker.Load()
	}
	return nil
}

// UnsettledRun is a run Unsettled reports: one that is not in a final state,
// or whose code may still be executing on a live executor.
type UnsettledRun struct {
	ID    string
	State State
	// Executing reports that the run's code may still be running: a live
	// executor has an open execution of it, or the run was cancelled after it
	// started on a live executor that recorded no execution of it (an
	// executor on a duro version without execution tracking, or a
	// hand-written workflow, which duro does not track). In the second case
	// it stays true until that executor stops.
	Executing bool
}

// unsettledSQL reads, in one statement and therefore one snapshot, each run's
// state and whether its code may still be executing.
//
// An execution counts while its row is open and its executor's lease is live,
// judged by the stale threshold that executor heartbeats with. A lease that is
// stale, tombstoned or absent means the process is gone, and its goroutines
// with it. Absent is safe to read as gone because execution tracking only runs
// in worker-pool mode, where every executor holds a lease before it launches,
// and a lease is pruned only long after it went stale.
//
// Reading the state and the rows in one snapshot is what makes the answer
// exact. An execution writes its row before its first stage, and DBOS checks
// for cancellation as each stage starts. So a stage running at the time of the
// snapshot either has a row in it, or started after it, and a stage that
// started after the snapshot started after the cancellation the snapshot
// shows, so it did not start.
//
// The second disjunct covers the executions no row describes. A run in
// SUCCESS or ERROR was written so by its executing goroutine after its code
// returned; only CANCELLED is written by someone else while the code may
// still run. A cancelled run that started (recovery_attempts > 0) and that no
// executor ever recorded was executed untracked (an older duro, a
// hand-written workflow), and counts as executing while the executor its row
// names is live. The test is "no row anywhere", not "no row on that
// executor": DBOS rewrites executor_id when a caller starts an existing
// workflow ID again, and a tracked run must not turn unsettled because of it.
//
// Two gaps remain, both for a fleet only partly on execution tracking (a
// rolling deploy): a run adopted from a tracking executor by one that does not
// track has rows, so its untracked execution is not seen; and an older duro's
// Parallel stage could finish a run while launched steps still ran. And one
// benign window: a run cancelled between its dequeue and its first start
// reads settled while DBOS still enters its body, which fails at its first
// stage without running it.
//
// Coupled to DBOS's workflow_status schema like takeover and Resume; a column
// rename fails here loudly.
const unsettledSQL = `SELECT ws.workflow_uuid, ws.status,
	EXISTS (
		SELECT 1 FROM ` + executionsTable + ` e
		JOIN ` + heartbeatTable + ` h ON h.executor_id = e.executor_id
		WHERE e.workflow_uuid = ws.workflow_uuid
		  AND e.ended_at IS NULL
		  AND h.last_heartbeat >= now() - h.stale_after_ms * interval '1 millisecond'
	) OR (
		ws.status = $2
		AND ws.recovery_attempts > 0
		AND EXISTS (
			SELECT 1 FROM ` + heartbeatTable + ` h
			WHERE h.executor_id = ws.executor_id
			  AND h.last_heartbeat >= now() - h.stale_after_ms * interval '1 millisecond'
		)
		AND NOT EXISTS (
			SELECT 1 FROM ` + executionsTable + ` e
			WHERE e.workflow_uuid = ws.workflow_uuid
		)
	) AS executing
	FROM dbos.workflow_status ws
	WHERE ws.workflow_uuid = ANY($1)`

// queryFunc runs one query against the system database. The App and the
// Client each build one from their own pool (poolQuery).
type queryFunc func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)

func poolQuery(pool *pgxpool.Pool) queryFunc {
	return func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return pool.Query(ctx, sql, args...)
	}
}

// unsettledRuns is the core behind App.Unsettled and Client.Unsettled.
func unsettledRuns(ctx context.Context, query queryFunc, workflowIDs []string) ([]UnsettledRun, error) {
	if len(workflowIDs) == 0 {
		return nil, nil
	}
	rows, err := query(ctx, unsettledSQL, workflowIDs, string(dbos.WorkflowStatusCancelled))
	if err != nil {
		return nil, fmt.Errorf("duro: checking whether runs are settled: %w", err)
	}
	type row struct {
		id        string
		status    string
		executing bool
	}
	read, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var out row
		err := r.Scan(&out.id, &out.status, &out.executing)
		return out, err
	})
	if err != nil {
		return nil, fmt.Errorf("duro: checking whether runs are settled: %w", err)
	}

	found := make(map[string]row, len(read))
	for _, r := range read {
		found[r.id] = r
	}
	var unsettled []UnsettledRun
	seen := make(map[string]bool, len(workflowIDs))
	for _, id := range workflowIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		r, ok := found[id]
		if !ok {
			return nil, fmt.Errorf("duro: checking whether runs are settled: %s: %w", id, ErrRunNotFound)
		}
		state := stateOf(dbos.WorkflowStatusType(r.status))
		if !state.Terminal() || r.executing {
			unsettled = append(unsettled, UnsettledRun{ID: id, State: state, Executing: r.executing})
		}
	}
	return unsettled, nil
}

// errUnsettledNeedsWorkerPool is returned by App.Unsettled on an App without
// worker-pool mode, which does not track executions.
var errUnsettledNeedsWorkerPool = errors.New("duro: Unsettled requires worker-pool mode (WithWorkerPool), which tracks executions")

// Unsettled reports which of the given runs are unsettled: not yet in a final
// state, or possibly still executing code on a live executor. A run it does
// not return is final, and none of its code is running or can start again
// unless someone resumes it. This is the check to make before starting new
// work that must not overlap an old run, such as a replacement run writing
// the same records: a cancelled run is final at once, but the stage it was
// executing keeps running until it returns.
//
// The answer rests on execution tracking, which worker-pool mode enables on
// every executor: each execution of a registered pipeline is recorded, and a
// dead executor's executions stop counting when its lease goes stale. With
// the whole fleet in worker-pool mode the answer is exact for pipelines. A
// run cancelled mid-execution on an executor that recorded no execution of
// it (a duro version without tracking, or a hand-written workflow) counts as
// executing until that executor stops.
//
// A cancelled pipeline run stops executing within about a heartbeat interval
// when its stage honors its context: the executor cancels the context of a
// run it sees cancelled.
//
// Unknown IDs fail with ErrRunNotFound. Duplicate IDs are reported once; no
// IDs report nothing. The call is bounded by ctx's deadline, or by
// DefaultReadTimeout when it has none. Client.Unsettled is the same check from
// an enqueue-only process.
func (a *App) Unsettled(ctx context.Context, workflowIDs ...string) ([]UnsettledRun, error) {
	if ctx == nil {
		return nil, errors.New("duro: Unsettled requires a non-nil context")
	}
	if a.wp == nil || a.wp.tracker == nil {
		return nil, errUnsettledNeedsWorkerPool
	}
	bctx, done := boundContext(a.DBOSContext, ctx, DefaultReadTimeout)
	defer done()
	return unsettledRuns(bctx, poolQuery(a.wp.tracker.pool), workflowIDs)
}
