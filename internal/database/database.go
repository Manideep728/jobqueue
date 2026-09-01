// Package database owns the Postgres connection and schema migrations.
package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"
	_ "github.com/lib/pq" // registers the "postgres" driver with database/sql
)


type Config struct {
	URL             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}






func DefaultConfig(url string) Config {
	return Config{URL: url, MaxOpenConns: 25, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute}
}






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
