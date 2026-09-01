package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
