// Package jobs contains the job model and every SQL statement that moves a job
// between states. Keeping all state transitions in one package means there is
// exactly one place to audit when reasoning about correctness.
package jobs

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Status is the lifecycle state of a job.
//
//	                  +---------------------> CANCELED   (only from PENDING)
//	                  |
//	PENDING --claim--> RUNNING --complete--> COMPLETED
//	   ^                  |
//	   |                  +--fail/lease expiry--> PENDING   (attempts < max_attempts)
//	   |                  |
//	   +------------------+--fail/lease expiry--> FAILED    (attempts >= max_attempts)
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	// StatusCanceled is deliberately distinct from FAILED: "a human stopped this"
	// and "this job exhausted its retries" are different operational events and
	// collapsing them would make the job list misleading.
	StatusCanceled Status = "CANCELED"
)

// ValidStatus reports whether s is a status the database will accept.
func ValidStatus(s Status) bool {
	switch s {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

// TypeShell is the only executor implemented. New types are added by extending
// the worker's executor switch, not by changing the queue.
const TypeShell = "shell"

// Job mirrors one row of the jobs table.
//
// Nullable columns are pointers so that JSON renders them as null rather than as
// a zero value that a client could mistake for real data (a zero time.Time would
// serialize as year 1, an empty string as "no error" when there was no attempt).
type Job struct {
	ID             int64      `json:"id"`
	Type           string     `json:"type"`
	Payload        string     `json:"payload"`
	Status         Status     `json:"status"`
	Attempts       int        `json:"attempts"`
	MaxAttempts    int        `json:"max_attempts"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	AvailableAt    time.Time  `json:"available_at"`
	WorkerID       *string    `json:"worker_id"`
	LeaseUntil     *time.Time `json:"lease_until"`
	LastError      *string    `json:"last_error"`
	ResultStdout   *string    `json:"result_stdout"`
	ResultStderr   *string    `json:"result_stderr"`
	ResultExitCode *int       `json:"result_exit_code"`
	DurationMS     *int64     `json:"duration_ms"`
}

// Worker mirrors one row of the workers table.
type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	JobsProcessed int64     `json:"jobs_processed"`
	CurrentJobID  *int64    `json:"current_job_id"`
	// Alive is derived at query time, not stored: "alive" depends on how long ago
	// the last heartbeat was, which changes every second without any write.
	Alive bool `json:"alive"`
}

// Errors returned by the store. Handlers map these onto HTTP status codes, which
// is why they are sentinel values rather than ad-hoc strings.
var (
	ErrNotFound      = errors.New("job not found")
	ErrNotClaimable  = errors.New("job is not in a claimable state")
	ErrNotOwned      = errors.New("job is not RUNNING under this worker")
	ErrNotCancelable = errors.New("only PENDING jobs can be canceled")
	ErrNoJobs        = errors.New("no jobs available")
	ErrUnknownWorker = errors.New("unknown worker: register first")
)

// MaxPayloadBytes bounds a single job payload. Without a bound, one client can
// exhaust the server's memory and the database's disk with a single request.
const MaxPayloadBytes = 64 * 1024

// ValidateNew checks a job-creation request before it reaches the database.
// Validating here (rather than relying on CHECK constraints) lets the API return
// a precise 400 explaining what was wrong instead of a generic constraint error.
func ValidateNew(typ, payload string, maxAttempts int) error {
	typ = strings.TrimSpace(typ)
	switch {
	case typ == "":
		return errors.New("type is required")
	case typ != TypeShell:
		return fmt.Errorf("unsupported job type %q (supported: %q)", typ, TypeShell)
	case strings.TrimSpace(payload) == "":
		return errors.New("payload is required")
	case len(payload) > MaxPayloadBytes:
		return fmt.Errorf("payload exceeds %d bytes", MaxPayloadBytes)
	case maxAttempts < 1:
		return errors.New("max_attempts must be >= 1")
	case maxAttempts > 100:
		return errors.New("max_attempts must be <= 100")
	}
	return nil
}

// BackoffConfig controls how long a failed job waits before it becomes claimable
// again. Exponential backoff stops a permanently-broken job from burning a worker
// in a hot loop, and spreads retries out so a struggling downstream dependency
// gets breathing room instead of a synchronized retry stampede.
type BackoffConfig struct {
	Base time.Duration // delay after the first failed attempt
	Max  time.Duration // ceiling, so attempt 20 does not wait for a year
}

// DefaultBackoff gives 1s, 2s, 4s, 8s ... capped at 5 minutes.
func DefaultBackoff() BackoffConfig {
	return BackoffConfig{Base: time.Second, Max: 5 * time.Minute}
}

// Delay returns the wait before the retry that follows `attempt` failed attempts.
// attempt is 1-based: Delay(1) is the wait after the first failure.
func (c BackoffConfig) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := c.Base
	if base <= 0 {
		base = time.Second
	}
	max := c.Max
	if max <= 0 {
		max = 5 * time.Minute
	}
	// Shifting by a large attempt count would overflow int64 and wrap to a
	// negative duration, so clamp the exponent before computing the shift.
	if attempt > 40 {
		return max
	}
	d := time.Duration(math.Pow(2, float64(attempt-1))) * base
	if d <= 0 || d > max {
		return max
	}
	return d
}
