package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)


type Store struct {
	db      *sql.DB
	backoff BackoffConfig
}


func NewStore(db *sql.DB, backoff BackoffConfig) *Store {
	return &Store{db: db, backoff: backoff}
}



const jobCols = `id, type, payload, status, attempts, max_attempts, created_at,
	updated_at, available_at, worker_id, lease_until, last_error,
	result_stdout, result_stderr, result_exit_code, duration_ms`



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






func (s *Store) Create(ctx context.Context, typ, payload string, maxAttempts int) (*Job, error) {
	const q = `INSERT INTO jobs (type, payload, max_attempts) VALUES ($1, $2, $3) RETURNING ` + jobCols
	return scanJob(s.db.QueryRowContext(ctx, q, typ, payload, maxAttempts))
}


func (s *Store) Get(ctx context.Context, id int64) (*Job, error) {
	const q = `SELECT ` + jobCols + ` FROM jobs WHERE id = $1`
	j, err := scanJob(s.db.QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}





func (s *Store) List(ctx context.Context, status Status, limit, offset int) ([]*Job, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	
	
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
			return ErrNoJobs 
		}
		if err != nil {
			return err
		}
		
		
		
		
		
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



func (s *Store) explainLostRow(ctx context.Context, id int64) error {
	if _, err := s.Get(ctx, id); err != nil {
		return err 
	}
	return ErrNotOwned
}






type Result struct {
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	ExitCode   *int   `json:"exit_code"`
	DurationMS *int64 `json:"duration_ms"`
	Error      string `json:"error"`
}


const MaxResultBytes = 32 * 1024



func TruncateOutput(s string) string {
	if len(s) <= MaxResultBytes {
		return s
	}
	return s[:MaxResultBytes] + "\n...[truncated]"
}











func (s *Store) Complete(ctx context.Context, id int64, workerID string, res Result) (*Job, error) {
	const q = `UPDATE jobs SET
			status = 'COMPLETED', worker_id = $2, lease_until = NULL, updated_at = now(),
			result_stdout = $3, result_stderr = $4, result_exit_code = $5,
			duration_ms = $6, last_error = NULL
		WHERE id = $1 AND status = 'RUNNING' AND worker_id = $2
		RETURNING ` + jobCols
	return s.finish(ctx, q, id, workerID, res, StatusCompleted)
}




func (s *Store) finish(ctx context.Context, q string, id int64, workerID string, res Result, want Status) (*Job, error) {
	var job *Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		j, err := scanJob(tx.QueryRowContext(ctx, q, id, workerID,
			TruncateOutput(res.Stdout), TruncateOutput(res.Stderr), res.ExitCode, res.DurationMS))
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
	if errors.Is(err, ErrNoJobs) {
		return s.replayOrConflict(ctx, id, workerID, want)
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}


func (s *Store) replayOrConflict(ctx context.Context, id int64, workerID string, want Status) (*Job, error) {
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err 
	}
	
	if cur.Status == want && cur.WorkerID != nil && *cur.WorkerID == workerID {
		return cur, nil
	}
	
	
	if want == StatusFailed && cur.Status == StatusPending && cur.WorkerID == nil {
		return cur, nil
	}
	return nil, ErrNotOwned
}





func (s *Store) Fail(ctx context.Context, id int64, workerID string, res Result) (*Job, error) {
	
	
	
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
			return cur, nil 
		}
		return nil, ErrNotCancelable
	}
	return j, err
}
