// Command queue-worker claims jobs from the queue server and executes them.
//
// SECURITY: a worker runs arbitrary shell commands taken from the queue. Point
// it only at a server you trust, and prefer running it as an unprivileged user.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/manideep7286/queue/internal/worker"
)

func main() {
	var (
		server    = flag.String("server", env("QUEUE_SERVER", "http://localhost:8080"), "queue server base URL")
		id        = flag.String("id", env("QUEUE_WORKER_ID", ""), "worker id (default: hostname-<random>)")
		lease     = flag.Int("lease-seconds", envInt("QUEUE_LEASE_SECONDS", 30), "how long to reserve a claimed job")
		poll      = flag.Float64("poll-seconds", envFloat("QUEUE_POLL_SECONDS", 1), "delay between claim attempts when idle")
		heartbeat = flag.Int("heartbeat-seconds", envInt("QUEUE_HEARTBEAT_SECONDS", 10), "heartbeat interval")
		timeout   = flag.Int("job-timeout-seconds", envInt("QUEUE_JOB_TIMEOUT_SECONDS", 300), "kill a job that runs longer than this")
		logLevel  = flag.String("log-level", env("QUEUE_LOG_LEVEL", "info"), "debug, info, warn or error")
		logFormat = flag.String("log-format", env("QUEUE_LOG_FORMAT", "text"), "text or json")
	)
	flag.Parse()

	workerID := *id
	if workerID == "" {
		workerID = generateID()
	}
	setupLogging(*logLevel, *logFormat, workerID)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// SIGINT/SIGTERM cancels the context, which stops the claim loop and kills
	// the running command; the worker then reports the failure so the job is
	// retried immediately rather than waiting out its lease.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := worker.New(worker.Config{
		ServerURL:         *server,
		WorkerID:          workerID,
		Hostname:          hostname,
		Token:             os.Getenv("QUEUE_AUTH_TOKEN"),
		Lease:             time.Duration(*lease) * time.Second,
		PollInterval:      time.Duration(*poll * float64(time.Second)),
		HeartbeatInterval: time.Duration(*heartbeat) * time.Second,
		JobTimeout:        time.Duration(*timeout) * time.Second,
	})

	if err := w.Run(ctx); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

// generateID builds a per-process identity: the hostname makes logs readable,
// the random suffix keeps several workers on one machine distinct.
func generateID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		// Fall back to the pid, which is unique on this machine right now.
		return host + "-" + strconv.Itoa(os.Getpid())
	}
	return host + "-" + hex.EncodeToString(b)
}

func setupLogging(level, format, workerID string) {
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
	slog.SetDefault(slog.New(h).With("component", "worker"))
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
