package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Store is the only thing in the program that talks SQL.
type Store struct {
	db      *sql.DB
	backoff BackoffConfig
}

// NewStore returns a Store using the given retry backoff policy.
func NewStore(db *sql.DB, backoff BackoffConfig) *Store {
	return &Store{db: db, backoff: backoff}
}

// jobCols is listed explicitly rather than using SELECT *, so that adding a
// column to the table cannot silently break every Scan in this file.
const jobCols = `id, type, payload, status, attempts, max_attempts, created_at,
	updated_at, available_at, worker_id, lease_until, last_error,
	result_stdout, result_stderr, result_exit_code, duration_ms`

// scanner is satisfied by both *sql.Row and *sql.Rows, so one scan helper serves
// single-row and multi-row queries.
type scanner interface{ Scan(dest ...any) error }

func scanJob(s scanner) (*Job, error) {
	var j Job
	err := s.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.CreatedAt, &j.UpdatedAt, &j.AvailableAt, &j.WorkerID, &j.LeaseUntil,
		&j.LastError, &j.ResultStdout, &j.ResultStderr, &j.ResultExitCode, &j.DurationMS)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// withTx runs fn inside a transaction, rolling back on error or panic.
//
// The deferred Rollback after a successful Commit is a no-op that returns
// sql.ErrTxDone, which is why its error is ignored: it exists to cover the
// early-return and panic paths.
func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Job creation and reads
// ---------------------------------------------------------------------------

// Create inserts a new PENDING job that is immediately claimable.
func (s *Store) Create(ctx context.Context, typ, payload string, maxAttempts int) (*Job, error) {
	const q = `INSERT INTO jobs (type, payload, max_attempts) VALUES ($1, $2, $3) RETURNING ` + jobCols
	return scanJob(s.db.QueryRowContext(ctx, q, typ, payload, maxAttempts))
}

// Get returns one job, or ErrNotFound.
func (s *Store) Get(ctx context.Context, id int64) (*Job, error) {
	const q = `SELECT ` + jobCols + ` FROM jobs WHERE id = $1`
	j, err := scanJob(s.db.QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// List returns jobs newest-first, optionally filtered by status.
//
// limit is always applied: an unbounded list endpoint is a denial-of-service
// waiting to happen once the table has a million rows.
func (s *Store) List(ctx context.Context, status Status, limit, offset int) ([]*Job, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	// $1 is NULL when no filter is wanted; the OR short-circuits to "all rows".
	// One query beats two near-identical strings that can drift apart.
	const q = `SELECT ` + jobCols + ` FROM jobs
		WHERE ($1::text IS NULL OR status = $1::text)
		ORDER BY id DESC LIMIT $2 OFFSET $3`

	var statusArg any
	if status != "" {
		statusArg = string(status)
	}
	rows, err := s.db.QueryContext(ctx, q, statusArg, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Non-nil empty slice so the API renders [] rather than null.
	out := make([]*Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// The claim: the one piece of this project that absolutely must be correct
// ---------------------------------------------------------------------------

// Claim atomically hands exactly one available job to one worker.
//
// How the race is prevented: the inner SELECT takes a row lock (FOR UPDATE) on
// the candidate row. SKIP LOCKED tells Postgres that if another transaction
// already holds that lock, do not block waiting for it -- skip past it to the
// next candidate. So N workers claiming concurrently each walk to a different
// row instead of serializing on the same one, and no two can ever return the
// same id. Without SKIP LOCKED the workers would still be correct but would
// queue up behind each other; without FOR UPDATE they could both read the same
// PENDING row and both update it.
//
// The whole thing is a single statement, so it is atomic even without an
// explicit transaction -- but we use one anyway to also stamp the worker's
// current_job_id in the same commit.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*Job, error) {
	const q = `
		UPDATE jobs SET
			status       = 'RUNNING',
			worker_id    = $1,
			lease_until  = now() + make_interval(secs => $2),
			attempts     = attempts + 1,
			updated_at   = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'PENDING' AND available_at <= now()
			ORDER BY available_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING ` + jobCols

	var job *Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		j, err := scanJob(tx.QueryRowContext(ctx, q, workerID, lease.Seconds()))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoJobs // an empty queue is normal, not an error condition
		}
		if err != nil {
			return err
		}
		// Registration is enforced here rather than being optional: if an
		// unknown worker could claim jobs, the workers table (and therefore
		// heartbeat-based liveness) would silently describe only some of the
		// fleet. Returning an error rolls the transaction back, so the job is
		// never left RUNNING under a worker nobody is tracking.
		res, err := tx.ExecContext(ctx,
			`UPDATE workers SET current_job_id = $1, last_heartbeat = now() WHERE id = $2`,
			j.ID, workerID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrUnknownWorker
		}
		job = j
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// RenewLease extends the lease of a job the caller still owns. A long-running
// job calls this on a timer; if the worker dies the calls stop, the lease
// lapses, and the reaper recovers the job.
func (s *Store) RenewLease(ctx context.Context, id int64, workerID string, lease time.Duration) (*Job, error) {
	const q = `UPDATE jobs SET lease_until = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND status = 'RUNNING' AND worker_id = $2
		RETURNING ` + jobCols
	j, err := scanJob(s.db.QueryRowContext(ctx, q, id, workerID, lease.Seconds()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, s.explainLostRow(ctx, id)
	}
	return j, err
}

// explainLostRow turns "the guarded UPDATE matched nothing" into a specific
// error, so the API can answer 404 vs 409 instead of a useless 500.
func (s *Store) explainLostRow(ctx context.Context, id int64) error {
	if _, err := s.Get(ctx, id); err != nil {
		return err // ErrNotFound, or a real database error
	}
	return ErrNotOwned
}

// ---------------------------------------------------------------------------
// Terminal transitions
// ---------------------------------------------------------------------------

// Result is what a worker reports back about an execution.
type Result struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   *int   `json:"exit_code"`
	DurationMS *int64 `json:"duration_ms"`
	Error      string `json:"error"`
}

// MaxResultBytes caps stored output so one chatty job cannot fill the disk.
const MaxResultBytes = 32 * 1024

// TruncateOutput shortens s to at most MaxResultBytes, marking that it was cut
// so nobody debugs a "silently truncated" log for an hour.
func TruncateOutput(s string) string {
	if len(s) <= MaxResultBytes {
		return s
	}
	return s[:MaxResultBytes] + "\n...[truncated]"
}

// Complete marks a job COMPLETED.
//
// The WHERE clause pins both status and worker_id. That guard is what makes a
// stale worker harmless: if its lease expired and another worker picked the job
// up, its late completion matches zero rows instead of overwriting the new
// owner's work.
//
// A repeat of a completion this same worker already made is treated as success,
// because the realistic cause is a retried HTTP request whose first response was
// lost -- the caller's intent already happened, so replaying it should not error.
func (s *Store) Complete(ctx context.Context, id int64, workerID string, res Result) (*Job, error) {
	const q = `UPDATE jobs SET
			status = 'COMPLETED', worker_id = $2, lease_until = NULL, updated_at = now(),
			result_stdout = $3, result_stderr = $4, result_exit_code = $5,
			duration_ms = $6, last_error = NULL
		WHERE id = $1 AND status = 'RUNNING' AND worker_id = $2
		RETURNING ` + jobCols
	return s.finish(ctx, q, id, workerID, res, StatusCompleted)
}

// finish runs a guarded terminal UPDATE, bumps the worker's counters in the same
// transaction, and falls back to idempotent replay detection when it matches
// nothing.
func (s *Store) finish(ctx context.Context, q string, id int64, workerID string, res Result, want Status) (*Job, error) {
	var job *Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		j, err := scanJob(tx.QueryRowContext(ctx, q, id, workerID,
			TruncateOutput(res.Stdout), TruncateOutput(res.Stderr), res.ExitCode, res.DurationMS))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoJobs // sentinel meaning "did not match"; resolved below
		}
		if err != nil {
			return err
		}
		// jobs_processed counts finished attempts by this worker, and clearing
		// current_job_id marks it idle again. Same transaction as the job update,
		// so the two can never disagree.
		if _, err := tx.ExecContext(ctx,
			`UPDATE workers SET jobs_processed = jobs_processed + 1, current_job_id = NULL,
				last_heartbeat = now() WHERE id = $1`, workerID); err != nil {
			return err
		}
		job = j
		return nil
	})
	if errors.Is(err, ErrNoJobs) {
		return s.replayOrConflict(ctx, id, workerID, want)
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

// replayOrConflict decides why a guarded terminal update matched no row.
func (s *Store) replayOrConflict(ctx context.Context, id int64, workerID string, want Status) (*Job, error) {
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err // ErrNotFound or a database error
	}
	// Same worker, already in the state it asked for: a duplicate request.
	if cur.Status == want && cur.WorkerID != nil && *cur.WorkerID == workerID {
		return cur, nil
	}
	// A retry that already bounced back to PENDING also counts as a replay of
	// this worker's own failure report.
	if want == StatusFailed && cur.Status == StatusPending && cur.WorkerID == nil {
		return cur, nil
	}
	return nil, ErrNotOwned
}

// Fail records a failed attempt and decides between retrying and giving up.
//
// attempts was already incremented at claim time, so "attempts >= max_attempts"
// means this failure was the last allowed one.
func (s *Store) Fail(ctx context.Context, id int64, workerID string, res Result) (*Job, error) {
	// Read attempts first so the retry delay comes from the same Go backoff
	// function the reaper uses -- reimplementing the formula in SQL would let the
	// two drift apart.
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	delay := s.backoff.Delay(cur.Attempts)

	const q = `UPDATE jobs SET
			status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
			-- a job going back to PENDING must release its worker, or the next
			-- claim would hand it out with a stale owner attached
			worker_id = CASE WHEN attempts >= max_attempts THEN $2 ELSE NULL END,
			lease_until = NULL,
			available_at = now() + make_interval(secs => $7),
			updated_at = now(),
			last_error = $8,
			result_stdout = $3, result_stderr = $4, result_exit_code = $5, duration_ms = $6
		WHERE id = $1 AND status = 'RUNNING' AND worker_id = $2
		RETURNING ` + jobCols

	var job *Job
	txErr := s.withTx(ctx, func(tx *sql.Tx) error {
		j, err := scanJob(tx.QueryRowContext(ctx, q, id, workerID,
			TruncateOutput(res.Stdout), TruncateOutput(res.Stderr), res.ExitCode,
			res.DurationMS, delay.Seconds(), TruncateOutput(res.Error)))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoJobs
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE workers SET jobs_processed = jobs_processed + 1, current_job_id = NULL,
				last_heartbeat = now() WHERE id = $1`, workerID); err != nil {
			return err
		}
		job = j
		return nil
	})
	if errors.Is(txErr, ErrNoJobs) {
		return s.replayOrConflict(ctx, id, workerID, StatusFailed)
	}
	if txErr != nil {
		return nil, txErr
	}
	return job, nil
}

// Cancel stops a job that has not started yet. A RUNNING job is deliberately not
// cancelable: the queue cannot reach into another process and kill a command it
// already started, so claiming to have canceled it would be a lie.
func (s *Store) Cancel(ctx context.Context, id int64) (*Job, error) {
	const q = `UPDATE jobs SET status = 'CANCELED', updated_at = now(), lease_until = NULL
		WHERE id = $1 AND status = 'PENDING' RETURNING ` + jobCols
	j, err := scanJob(s.db.QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		cur, gerr := s.Get(ctx, id)
		if gerr != nil {
			return nil, gerr
		}
		if cur.Status == StatusCanceled {
			return cur, nil // idempotent: canceling a canceled job is a no-op
		}
		return nil, ErrNotCancelable
	}
	return j, err
}

// ---------------------------------------------------------------------------
// Lease recovery
// ---------------------------------------------------------------------------

// Reaped describes one job the reaper rescued, for logging.
type Reaped struct {
	ID       int64
	Attempts int
	Status   Status // where it landed: PENDING (retry) or FAILED (exhausted)
	Worker   string
}

// ReapExpiredLeases returns orphaned jobs to the queue.
//
// This is the failure-recovery story: if a worker is killed -9, unplugged, or
// partitioned off the network, nobody ever reports on its job. The only evidence
// available to the server is that the lease stopped being renewed. Once it
// lapses, the job is either retried or, if it is out of attempts, failed.
//
// SKIP LOCKED again means several server instances can reap concurrently without
// stepping on each other, and the batch limit keeps one sweep bounded.
func (s *Store) ReapExpiredLeases(ctx context.Context, batch int) ([]Reaped, error) {
	if batch <= 0 {
		batch = 100
	}
	var out []Reaped
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT id, attempts, max_attempts, coalesce(worker_id, '')
			FROM jobs WHERE status = 'RUNNING' AND lease_until < now()
			ORDER BY lease_until
			FOR UPDATE SKIP LOCKED LIMIT $1`, batch)
		if err != nil {
			return err
		}
		type expired struct {
			id                 int64
			attempts, maxAttem int
			worker             string
		}
		var found []expired
		for rows.Next() {
			var e expired
			if err := rows.Scan(&e.id, &e.attempts, &e.maxAttem, &e.worker); err != nil {
				rows.Close()
				return err
			}
			found = append(found, e)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		// Close before issuing writes on the same transaction: a *sql.Tx runs one
		// statement at a time, and an open Rows still holds it.
		rows.Close()

		for _, e := range found {
			next := StatusPending
			if e.attempts >= e.maxAttem {
				next = StatusFailed
			}
			delay := s.backoff.Delay(e.attempts)
			// Re-check status inside the write: the row is locked, but being
			// explicit keeps this correct if the query above is ever relaxed.
			if _, err := tx.ExecContext(ctx, `
				UPDATE jobs SET status = $2,
					worker_id = CASE WHEN $2 = 'FAILED' THEN worker_id ELSE NULL END,
					lease_until = NULL,
					available_at = now() + make_interval(secs => $3),
					last_error = $4, updated_at = now()
				WHERE id = $1 AND status = 'RUNNING'`,
				e.id, string(next), delay.Seconds(),
				fmt.Sprintf("lease expired (worker %s stopped reporting)", e.worker)); err != nil {
				return err
			}
			// The dead worker is no longer running this job.
			if e.worker != "" {
				if _, err := tx.ExecContext(ctx,
					`UPDATE workers SET current_job_id = NULL WHERE id = $1 AND current_job_id = $2`,
					e.worker, e.id); err != nil {
					return err
				}
			}
			out = append(out, Reaped{ID: e.id, Attempts: e.attempts, Status: next, Worker: e.worker})
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// RegisterWorker upserts a worker row. Upsert rather than insert so that a
// worker restarting with the same id resumes its identity instead of crashing on
// a duplicate key.
func (s *Store) RegisterWorker(ctx context.Context, id, hostname string) (*Worker, error) {
	const q = `INSERT INTO workers (id, hostname) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET hostname = EXCLUDED.hostname,
			last_heartbeat = now(), current_job_id = NULL
		RETURNING id, hostname, registered_at, last_heartbeat, jobs_processed, current_job_id`
	var w Worker
	err := s.db.QueryRowContext(ctx, q, id, hostname).Scan(&w.ID, &w.Hostname,
		&w.RegisteredAt, &w.LastHeartbeat, &w.JobsProcessed, &w.CurrentJobID)
	if err != nil {
		return nil, err
	}
	w.Alive = true
	return &w, nil
}

// Heartbeat records liveness. ErrNotFound tells an unknown worker to register,
// which is how a worker recovers after the database was reset underneath it.
func (s *Store) Heartbeat(ctx context.Context, id string) (*Worker, error) {
	const q = `UPDATE workers SET last_heartbeat = now() WHERE id = $1
		RETURNING id, hostname, registered_at, last_heartbeat, jobs_processed, current_job_id`
	var w Worker
	err := s.db.QueryRowContext(ctx, q, id).Scan(&w.ID, &w.Hostname,
		&w.RegisteredAt, &w.LastHeartbeat, &w.JobsProcessed, &w.CurrentJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	w.Alive = true
	return &w, nil
}

// ListWorkers returns all known workers, marking as alive those that have sent a
// heartbeat within timeout.
func (s *Store) ListWorkers(ctx context.Context, timeout time.Duration) ([]*Worker, error) {
	const q = `SELECT id, hostname, registered_at, last_heartbeat, jobs_processed,
			current_job_id, last_heartbeat > now() - make_interval(secs => $1)
		FROM workers ORDER BY registered_at`
	rows, err := s.db.QueryContext(ctx, q, timeout.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*Worker, 0, 8)
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.Hostname, &w.RegisteredAt, &w.LastHeartbeat,
			&w.JobsProcessed, &w.CurrentJobID, &w.Alive); err != nil {
			return nil, err
		}
		out = append(out, &w)
	}
	return out, rows.Err()
}

// Stats is the queue depth by status, for the health endpoint and the CLI.
type Stats map[string]int64

// Stats counts jobs grouped by status.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := Stats{}
	// Seed every status so a client always sees the full set of keys, including
	// the zeros -- an absent key is easy to misread as "unknown" rather than "none".
	for _, st := range []Status{StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCanceled} {
		out[string(st)] = 0
	}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
