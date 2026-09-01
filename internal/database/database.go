// Package database owns the Postgres connection and schema migrations.
package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver with database/sql

	"github.com/manideep7286/queue/migrations"
)

// Config controls the connection pool.
type Config struct {
	URL             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// DefaultConfig returns pool settings sized for this project.
//
// The pool matters here: every worker poll is a database round trip, so an
// unbounded pool would let a burst of workers open hundreds of Postgres
// backends (each of which costs real memory) and tip the database over.
func DefaultConfig(url string) Config {
	return Config{URL: url, MaxOpenConns: 25, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute}
}

// Open connects and verifies the connection is actually usable.
//
// sql.Open does not connect -- it only validates the DSN and returns a lazy
// pool. Without the Ping, a bad password would surface as a confusing error on
// the first request instead of at startup.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return db, nil
}

// WaitForDB retries the connection until it succeeds or the timeout elapses.
//
// Needed because docker compose starts the server and Postgres at the same
// time, and Postgres takes a few seconds to accept connections. Crash-looping
// until the database appears would also work, but produces alarming logs.
func WaitForDB(ctx context.Context, cfg Config, timeout time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for attempt := 1; ; attempt++ {
		db, err := Open(ctx, cfg)
		if err == nil {
			return db, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("database unreachable after %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Migrate applies every embedded migration that has not run yet.
//
// The ledger table plus filename ordering gives the two properties that matter:
// running Migrate twice is a no-op, and every environment ends up with the same
// schema in the same order.
func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	names, err := migrationNames()
	if err != nil {
		return err
	}

	for _, name := range names {
		var exists bool
		if err := db.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if exists {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		// One transaction per migration: a migration that fails halfway leaves no
		// partial schema behind, and is not recorded, so it is retried next boot.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // 001_, 002_, ... numeric prefixes make this the intended order
	return names, nil
}
