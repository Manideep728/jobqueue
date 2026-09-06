// Command queue-server runs the HTTP queue service.
//
// SECURITY: this queue executes shell commands submitted through its API. It is
// a remote code execution service by design and must never be exposed to an
// untrusted network. The default listen address is therefore 127.0.0.1.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/manideep7286/queue/internal/api"
	"github.com/manideep7286/queue/internal/database"
	"github.com/manideep7286/queue/internal/jobs"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr        = flag.String("addr", env("QUEUE_ADDR", "127.0.0.1:8080"), "listen address (host:port)")
		dbURL       = flag.String("database-url", env("DATABASE_URL", "postgres://queue:queue@localhost:5432/queue?sslmode=disable"), "PostgreSQL connection string")
		leaseSec    = flag.Int("lease-seconds", envInt("QUEUE_LEASE_SECONDS", 30), "default job lease duration")
		reapSec     = flag.Int("reap-seconds", envInt("QUEUE_REAP_SECONDS", 5), "how often to sweep for expired leases")
		hbSec       = flag.Int("heartbeat-timeout", envInt("QUEUE_HEARTBEAT_TIMEOUT", 60), "seconds without a heartbeat before a worker is considered dead")
		backoffSec  = flag.Float64("retry-base-seconds", envFloat("QUEUE_RETRY_BASE_SECONDS", 1), "base delay for exponential retry backoff")
		backoffMax  = flag.Float64("retry-max-seconds", envFloat("QUEUE_RETRY_MAX_SECONDS", 300), "ceiling for retry backoff")
		logLevel    = flag.String("log-level", env("QUEUE_LOG_LEVEL", "info"), "debug, info, warn or error")
		logFormat   = flag.String("log-format", env("QUEUE_LOG_FORMAT", "text"), "text or json")
		skipMigrate = flag.Bool("skip-migrate", false, "do not run migrations on startup")
	)
	flag.Parse()

	setupLogging(*logLevel, *logFormat)

	// The auth token comes only from the environment. A flag would put it in the
	// process list, where any other user on the machine could read it.
	token := os.Getenv("QUEUE_AUTH_TOKEN")

	// Signal-aware context: one Ctrl-C cancels everything downstream -- the
	// reaper, in-flight database calls, and the shutdown path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.WaitForDB(ctx, database.DefaultConfig(*dbURL), 30*time.Second)
	if err != nil {
		return err
	}
	defer db.Close()
	slog.Info("database_connected")

	if !*skipMigrate {
		if err := database.Migrate(ctx, db); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		slog.Info("migrations_applied")
	}

	backoff := jobs.BackoffConfig{
		Base: time.Duration(*backoffSec * float64(time.Second)),
		Max:  time.Duration(*backoffMax * float64(time.Second)),
	}
	store := jobs.NewStore(db, backoff)

	cfg := api.DefaultConfig()
	cfg.DefaultLease = time.Duration(*leaseSec) * time.Second
	cfg.HeartbeatTimeout = time.Duration(*hbSec) * time.Second
	cfg.AuthToken = token

	reaper := api.NewReaper(store, time.Duration(*reapSec)*time.Second, 100)
	go reaper.Run(ctx)

	srv := &http.Server{
		Handler: api.New(store, cfg),
		Addr:    *addr,
		// Timeouts are not optional on a server exposed to any network: without
		// them a single client that opens a connection and never sends anything
		// occupies a goroutine and a file descriptor indefinitely.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// ListenAndServe blocks, so it runs in a goroutine and reports back through
	// a channel; main then waits on either a startup failure or a signal.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("server_started", "addr", *addr, "auth_enabled", token != "",
			"lease_seconds", *leaseSec, "reap_seconds", *reapSec)
		if token == "" {
			slog.Warn("authentication disabled; this server executes shell commands. " +
				"Do not expose it beyond localhost without setting QUEUE_AUTH_TOKEN.")
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown_signal_received")
	}

	// Graceful shutdown: stop accepting connections, let in-flight requests
	// finish. A worker mid-report gets its answer instead of a dropped
	// connection, which would otherwise strand its job until the lease expires.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	slog.Info("server_stopped")
	return nil
}

func setupLogging(level, format string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	// Every log line carries a timestamp (slog adds it) plus the component, so
	// interleaved server and worker output stays readable.
	slog.SetDefault(slog.New(h).With("component", "server"))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
