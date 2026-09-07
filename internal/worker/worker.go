package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

// Config controls one worker process.
type Config struct {
	ServerURL string
	WorkerID  string
	Hostname  string
	Token     string

	// Lease is how long the worker reserves a job for. Short leases recover
	// faster from a crash but require more renewal traffic.
	Lease time.Duration
	// PollInterval is the wait between claim attempts when the queue is empty.
	PollInterval time.Duration
	// HeartbeatInterval must be comfortably shorter than the server's heartbeat
	// timeout, or a healthy worker gets reported dead between beats.
	HeartbeatInterval time.Duration
	// JobTimeout kills a command that runs too long. Without it, one hung job
	// occupies a worker forever.
	JobTimeout time.Duration
}

// Worker runs the claim-execute-report loop.
type Worker struct {
	cfg    Config
	client *Client
	log    *slog.Logger
}

// New builds a Worker.
func New(cfg Config) *Worker {
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 5 * time.Minute
	}
	return &Worker{
		cfg:    cfg,
		client: NewClient(cfg.ServerURL, cfg.Token),
		log:    slog.Default().With("worker_id", cfg.WorkerID),
	}
}

// Run works until ctx is canceled.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.registerWithRetry(ctx); err != nil {
		return err
	}
	w.log.Info("worker_started", "server", w.cfg.ServerURL,
		"lease_seconds", w.cfg.Lease.Seconds(), "job_timeout_seconds", w.cfg.JobTimeout.Seconds())

	go w.heartbeatLoop(ctx)

	for {
		// Checked at the top of every iteration so a shutdown signal is honored
		// even when the queue is busy and the loop never reaches a sleep.
		if ctx.Err() != nil {
			w.log.Info("worker_stopped")
			return nil
		}

		job, err := w.client.Claim(ctx, w.cfg.WorkerID, w.cfg.Lease)
		switch {
		case err == nil:
			w.process(ctx, job)
			// No sleep: go straight back for the next job while work exists.
			continue
		case errors.Is(err, ErrNoJobs):
			// Idle. Sleep with jitter so N workers started together do not all
			// poll on the same tick and hammer the database in lockstep.
			w.sleep(ctx, jitter(w.cfg.PollInterval))
		case errors.Is(err, ErrUnknownWorker):
			// The server forgot us -- typically its database was reset. Register
			// again rather than polling uselessly forever.
			w.log.Warn("worker_unknown_reregistering")
			if err := w.registerWithRetry(ctx); err != nil {
				return err
			}
		case ctx.Err() != nil:
			w.log.Info("worker_stopped")
			return nil
		default:
			// The server is down or unreachable. Keep retrying: a worker that
			// exits on a transient blip needs a human to restart it.
			w.log.Error("claim_failed", "error", err)
			w.sleep(ctx, jitter(2*w.cfg.PollInterval))
		}
	}
}

// registerWithRetry keeps trying to register until it succeeds or ctx ends,
// so `docker compose up` can start workers before the server is ready.
func (w *Worker) registerWithRetry(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := w.client.Register(ctx, w.cfg.WorkerID, w.cfg.Hostname)
		if err == nil {
			w.log.Info("worker_registered", "hostname", w.cfg.Hostname)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.log.Warn("registration_failed", "attempt", attempt, "error", err,
			"retry_in_seconds", backoff.Seconds())
		if !w.sleep(ctx, backoff) {
			return ctx.Err()
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

// heartbeatLoop tells the server this worker is alive.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.Heartbeat(ctx, w.cfg.WorkerID); err != nil {
				if ctx.Err() != nil {
					return
				}
				// A missed heartbeat is not fatal on its own; the worker is only
				// declared dead after the server's whole timeout elapses.
				w.log.Warn("heartbeat_failed", "error", err)
			}
		}
	}
}

// process executes one claimed job and reports the outcome.
func (w *Worker) process(ctx context.Context, job *jobs.Job) {
	log := w.log.With("job_id", job.ID, "attempt", job.Attempts, "max_attempts", job.MaxAttempts)
	log.Info("job_claimed", "type", job.Type, "payload", job.Payload)

	// jobCtx is canceled either by shutdown or by losing the lease.
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Renew at a third of the lease: two consecutive renewals can fail (a blip,
	// a slow request) before the lease actually lapses.
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		w.renewLoop(jobCtx, cancel, job.ID, log)
	}()

	log.Info("job_started")
	var (
		res ExecResult
		err error
	)
	switch job.Type {
	case jobs.TypeShell:
		res, err = ExecuteShell(jobCtx, job.Payload, w.cfg.JobTimeout)
	default:
		// Should be impossible -- the API validates the type on submission -- but
		// a worker running against a newer server must fail loudly, not silently
		// mark unknown work as done.
		err = fmt.Errorf("unsupported job type %q", job.Type)
	}

	cancel()    // stop renewing before reporting
	<-renewDone // and wait, so no renewal races the terminal update

	// Reports use a fresh context: during shutdown the parent ctx is already
	// canceled, and a report that cannot be sent leaves the job stranded until
	// its lease expires.
	reportCtx, reportCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer reportCancel()

	if err != nil {
		cause := err.Error()
		if jobCtx.Err() != nil && ctx.Err() != nil {
			cause = "worker shut down mid-execution: " + cause
		}
		log.Warn("job_failed", "error", cause, "duration_ms", res.DurationMS())
		w.report(reportCtx, func(c context.Context) error {
			return w.client.Fail(c, job.ID, w.cfg.WorkerID, res, cause)
		}, log, "fail")
		return
	}

	log.Info("job_completed", "duration_ms", res.DurationMS(), "exit_code", derefInt(res.ExitCode))
	w.report(reportCtx, func(c context.Context) error {
		return w.client.Complete(c, job.ID, w.cfg.WorkerID, res)
	}, log, "complete")
}

// renewLoop keeps the lease alive, and cancels the job if it cannot.
func (w *Worker) renewLoop(ctx context.Context, cancelJob context.CancelFunc, jobID int64, log *slog.Logger) {
	ticker := time.NewTicker(w.cfg.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := w.client.RenewLease(ctx, jobID, w.cfg.WorkerID, w.cfg.Lease)
			if err == nil {
				log.Debug("lease_renewed")
				continue
			}
			if ctx.Err() != nil {
				return
			}
			var apiErr *APIError
			// 409 means the server no longer considers us the owner: the lease
			// lapsed and the reaper handed the job to someone else. Abandoning
			// immediately is the whole point -- continuing would execute the job
			// twice, which is exactly what leases exist to prevent.
			if errors.As(err, &apiErr) && apiErr.Status == 409 {
				log.Warn("lease_lost", "detail", apiErr.Msg)
				cancelJob()
				return
			}
			log.Warn("lease_renew_failed", "error", err)
		}
	}
}

// report sends a terminal update, retrying transient failures.
//
// This matters more than it looks: a lost report means the job sits RUNNING
// until its lease expires and then gets executed all over again.
func (w *Worker) report(ctx context.Context, fn func(context.Context) error, log *slog.Logger, kind string) {
	backoff := 250 * time.Millisecond
	for attempt := 1; attempt <= 4; attempt++ {
		err := fn(ctx)
		if err == nil {
			return
		}
		var apiErr *APIError
		// A 4xx will never succeed on retry -- the job was reassigned, or is
		// already terminal. Log it and move on to the next job.
		if errors.As(err, &apiErr) && apiErr.Status < 500 {
			log.Warn("report_rejected", "kind", kind, "status", apiErr.Status, "detail", apiErr.Msg)
			return
		}
		log.Warn("report_failed", "kind", kind, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	log.Error("report_gave_up", "kind", kind,
		"consequence", "job will be recovered by the server when its lease expires")
}

// sleep waits for d, returning false if the context ended first.
func (w *Worker) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter spreads timers by +/-25% so workers do not synchronize into a
// thundering herd against the database.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := float64(d) * 0.25
	return time.Duration(float64(d) - delta + rand.Float64()*2*delta)
}

func derefInt(p *int) int {
	if p == nil {
		return -1
	}
	return *p
}
