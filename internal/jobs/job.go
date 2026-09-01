// Package jobs contains the job model and every SQL statement that moves a job
// between states. Keeping all state transitions in one package means there is
// exactly one place to audit when reasoning about correctness.
package jobs

import (
	"errors"
	"time"
)










type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	
	
	
	StatusCanceled Status = "CANCELED"
)


func ValidStatus(s Status) bool {
	switch s {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCanceled:
		return true
	}
	return false
}



const TypeShell = "shell"






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


type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	RegisteredAt  time.Time `json:"registered_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	JobsProcessed int64     `json:"jobs_processed"`
	CurrentJobID  *int64    `json:"current_job_id"`
	
	
	Alive bool `json:"alive"`
}



var (
	ErrNotFound      = errors.New("job not found")
	ErrNotClaimable  = errors.New("job is not in a claimable state")
	ErrNotOwned      = errors.New("job is not RUNNING under this worker")
	ErrNotCancelable = errors.New("only PENDING jobs can be canceled")
	ErrNoJobs        = errors.New("no jobs available")
	ErrUnknownWorker = errors.New("unknown worker: register first")
)
