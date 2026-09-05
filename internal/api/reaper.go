package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

// Reaper periodically returns jobs with expired leases to the queue.
//
// It lives in the server rather than the worker for the obvious reason: the
// scenario being recovered from is "the worker is gone". Only a process that
// outlives the worker can notice that its lease stopped being renewed.
//
// Running several servers is safe -- ReapExpiredLeases uses FOR UPDATE SKIP
// LOCKED, so two reapers sweeping at the same instant take disjoint rows.
type Reaper struct {
	store    *jobs.Store
	interval time.Duration
	batch    int
}

// NewReaper returns a Reaper sweeping every interval.
func NewReaper(store *jobs.Store, interval time.Duration, batch int) *Reaper {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Reaper{store: store, interval: interval, batch: batch}
}

// Run sweeps until ctx is canceled. It is meant to be started in a goroutine and
// stopped by canceling the context during shutdown.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	slog.Info("reaper_started", "interval_seconds", r.interval.Seconds())

	for {
		select {
		case <-ctx.Done():
			slog.Info("reaper_stopped")
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) {
	// A bounded timeout keeps one slow sweep from blocking every later tick, and
	// stops a hung query from pinning a connection forever.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	reaped, err := r.store.ReapExpiredLeases(ctx, r.batch)
	if err != nil {
		// A transient database error must not kill the loop: the next tick retries.
		if ctx.Err() == nil {
			slog.Error("reaper_sweep_failed", "error", err)
		}
		return
	}
	for _, j := range reaped {
		slog.Warn("lease_expired",
			"job_id", j.ID,
			"worker_id", j.Worker,
			"attempts", j.Attempts,
			"new_status", string(j.Status),
			"event", map[jobs.Status]string{
				jobs.StatusPending: "requeued_for_retry",
				jobs.StatusFailed:  "retries_exhausted",
			}[j.Status],
		)
	}
}
