package tests

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

// completeJob reports success for a job.
func (h *harness) completeJob(id int64, workerID string, out any) int {
	h.t.Helper()
	return h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/complete", id), map[string]any{
		"worker_id": workerID, "stdout": "done", "exit_code": ptrInt(0), "duration_ms": 12,
	}, out)
}

// failJob reports a failed attempt.
func (h *harness) failJob(id int64, workerID, cause string, out any) int {
	h.t.Helper()
	return h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/fail", id), map[string]any{
		"worker_id": workerID, "stderr": cause, "exit_code": ptrInt(1),
		"duration_ms": 5, "error": cause,
	}, out)
}

// Catches: a claim not moving the job to RUNNING, not incrementing attempts, not
// setting a lease, or a completed job not recording its result.
func TestHappyPathStateTransitions(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("echo hi", 3)

	var claimed jobs.Job
	if code := h.claim("w1", 30, &claimed); code != http.StatusOK {
		t.Fatalf("claim: status %d", code)
	}
	if claimed.ID != job.ID {
		t.Fatalf("claimed job %d, want %d", claimed.ID, job.ID)
	}
	if claimed.Status != jobs.StatusRunning {
		t.Errorf("status = %s, want RUNNING", claimed.Status)
	}
	if claimed.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: the attempt is counted at claim time so a "+
			"worker that dies without reporting still consumes one", claimed.Attempts)
	}
	if claimed.WorkerID == nil || *claimed.WorkerID != "w1" {
		t.Errorf("worker_id = %v, want w1", claimed.WorkerID)
	}
	if claimed.LeaseUntil == nil || !claimed.LeaseUntil.After(time.Now()) {
		t.Errorf("lease_until = %v, want a time in the future", claimed.LeaseUntil)
	}

	var done jobs.Job
	if code := h.completeJob(job.ID, "w1", &done); code != http.StatusOK {
		t.Fatalf("complete: status %d", code)
	}
	if done.Status != jobs.StatusCompleted {
		t.Errorf("status = %s, want COMPLETED", done.Status)
	}
	if done.LeaseUntil != nil {
		t.Error("a finished job must not keep a lease")
	}
	if done.ResultStdout == nil || *done.ResultStdout != "done" {
		t.Errorf("result_stdout = %v, want %q", done.ResultStdout, "done")
	}
	if done.ResultExitCode == nil || *done.ResultExitCode != 0 {
		t.Errorf("exit code = %v, want 0", done.ResultExitCode)
	}
	if done.DurationMS == nil || *done.DurationMS != 12 {
		t.Errorf("duration_ms = %v, want 12", done.DurationMS)
	}

	// A COMPLETED job must never be handed out again.
	var er map[string]string
	if code := h.claim("w1", 30, &er); code != http.StatusNotFound {
		t.Errorf("claim after completion = %d, want 404", code)
	}
}

// Catches: a failure going straight to FAILED instead of retrying, retries not
// being delayed by backoff, or the retry count not being enforced.
func TestRetryUntilMaxAttempts(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("exit 1", 3)

	for attempt := 1; attempt <= 3; attempt++ {
		// Wait out the backoff from the previous failure (10ms base in tests).
		var claimed jobs.Job
		code := h.claimWithin("w1", 5*time.Second, &claimed)
		if code != http.StatusOK {
			t.Fatalf("attempt %d: claim returned %d, want the job to become available again", attempt, code)
		}
		if claimed.Attempts != attempt {
			t.Errorf("attempt %d: attempts = %d", attempt, claimed.Attempts)
		}

		var after jobs.Job
		if code := h.failJob(job.ID, "w1", "boom", &after); code != http.StatusOK {
			t.Fatalf("attempt %d: fail returned %d", attempt, code)
		}

		if attempt < 3 {
			if after.Status != jobs.StatusPending {
				t.Fatalf("attempt %d: status = %s, want PENDING (retries remain)", attempt, after.Status)
			}
			if !after.AvailableAt.After(after.UpdatedAt) {
				t.Errorf("attempt %d: available_at %v is not after updated_at %v; backoff was not applied",
					attempt, after.AvailableAt, after.UpdatedAt)
			}
			if after.WorkerID != nil {
				t.Errorf("attempt %d: a requeued job must release its worker, got %v", attempt, *after.WorkerID)
			}
			if after.LeaseUntil != nil {
				t.Errorf("attempt %d: a requeued job must release its lease", attempt)
			}
		} else {
			if after.Status != jobs.StatusFailed {
				t.Fatalf("final attempt: status = %s, want FAILED", after.Status)
			}
			if after.LastError == nil || *after.LastError != "boom" {
				t.Errorf("last_error = %v, want %q", after.LastError, "boom")
			}
		}
	}

	// An exhausted job must stay out of the queue permanently.
	time.Sleep(50 * time.Millisecond)
	var er map[string]string
	if code := h.claim("w1", 30, &er); code != http.StatusNotFound {
		t.Errorf("claim after retry exhaustion = %d, want 404", code)
	}
}

// Catches: max_attempts=1 still retrying, which would silently double every
// non-idempotent job the user explicitly asked to run only once.
func TestMaxAttemptsOneFailsImmediately(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("exit 1", 1)

	var claimed jobs.Job
	h.claim("w1", 30, &claimed)
	var after jobs.Job
	h.failJob(job.ID, "w1", "boom", &after)
	if after.Status != jobs.StatusFailed {
		t.Errorf("status = %s, want FAILED with max_attempts=1", after.Status)
	}
}

// Catches: a retried HTTP request (whose first response was lost) turning into a
// 409 the worker cannot act on, or worse, double-counting the job.
func TestDuplicateCompletionIsIdempotent(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("echo hi", 3)
	var claimed jobs.Job
	h.claim("w1", 30, &claimed)

	var first jobs.Job
	if code := h.completeJob(job.ID, "w1", &first); code != http.StatusOK {
		t.Fatalf("first complete: %d", code)
	}
	var second jobs.Job
	if code := h.completeJob(job.ID, "w1", &second); code != http.StatusOK {
		t.Fatalf("duplicate complete: %d, want 200 (idempotent replay)", code)
	}
	if second.Status != jobs.StatusCompleted {
		t.Errorf("status = %s, want COMPLETED", second.Status)
	}

	// The duplicate must not be counted as a second finished job.
	var list struct {
		Workers []jobs.Worker `json:"workers"`
	}
	h.do(http.MethodGet, "/workers", nil, &list)
	if list.Workers[0].JobsProcessed != 1 {
		t.Errorf("jobs_processed = %d, want 1: a duplicate report must not double-count",
			list.Workers[0].JobsProcessed)
	}
}

// Catches: THE dangerous one. A worker whose lease expired finishes late and
// reports success, overwriting the result of the worker that legitimately took
// the job over. The guard is `WHERE worker_id = $x` on every terminal update.
func TestStaleWorkerCannotOverwriteReassignedJob(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	h.registerWorker("w2")
	job := h.createJob("echo hi", 3)

	// w1 claims with a lease that has already effectively expired.
	var claimed jobs.Job
	if code := h.claim("w1", 0.001, &claimed); code != http.StatusOK {
		t.Fatalf("claim: %d", code)
	}
	time.Sleep(50 * time.Millisecond)

	// The reaper returns it to the queue and w2 picks it up.
	if _, err := h.store.ReapExpiredLeases(context.Background(), 10); err != nil {
		t.Fatalf("reap: %v", err)
	}
	// A reaped job carries the normal retry backoff, so wait for it to elapse
	// rather than assuming it is instantly claimable.
	var reclaimed jobs.Job
	if code := h.claimWithin("w2", 3*time.Second, &reclaimed); code != http.StatusOK {
		t.Fatalf("w2 claim: %d, want the expired job to be available again", code)
	}
	if reclaimed.ID != job.ID {
		t.Fatalf("w2 claimed job %d, want %d", reclaimed.ID, job.ID)
	}

	// Now w1 finally finishes and reports. It must be rejected.
	var er map[string]string
	if code := h.completeJob(job.ID, "w1", &er); code != http.StatusConflict {
		t.Fatalf("stale worker complete = %d, want 409", code)
	}

	// And the job must still belong to w2.
	current := h.getJob(job.ID)
	if current.Status != jobs.StatusRunning {
		t.Errorf("status = %s, want RUNNING under w2", current.Status)
	}
	if current.WorkerID == nil || *current.WorkerID != "w2" {
		t.Errorf("worker_id = %v, want w2: the stale worker overwrote the owner", current.WorkerID)
	}

	// w2's own completion still works.
	if code := h.completeJob(job.ID, "w2", nil); code != http.StatusOK {
		t.Errorf("w2 complete = %d, want 200", code)
	}
}

// Catches: reports for a job the worker never held being accepted, and reports
// for a nonexistent job returning 500 instead of 404.
func TestReportValidation(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	h.registerWorker("w2")
	job := h.createJob("echo hi", 3)

	var er map[string]string
	// Nobody has claimed it yet.
	if code := h.completeJob(job.ID, "w1", &er); code != http.StatusConflict {
		t.Errorf("complete an unclaimed job = %d, want 409", code)
	}

	var claimed jobs.Job
	h.claim("w1", 30, &claimed)
	if code := h.completeJob(job.ID, "w2", &er); code != http.StatusConflict {
		t.Errorf("complete another worker's job = %d, want 409", code)
	}
	if code := h.completeJob(999999, "w1", &er); code != http.StatusNotFound {
		t.Errorf("complete a nonexistent job = %d, want 404", code)
	}
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/complete", job.ID),
		map[string]any{"stdout": "x"}, &er); code != http.StatusBadRequest {
		t.Errorf("complete without worker_id = %d, want 400", code)
	}
}

// Catches: renewal not actually extending the lease (so a long job gets reaped
// mid-execution), or a worker being able to renew a lease it does not hold.
func TestLeaseRenewal(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	h.registerWorker("w2")
	job := h.createJob("sleep 10", 3)

	var claimed jobs.Job
	h.claim("w1", 1, &claimed)
	firstLease := *claimed.LeaseUntil

	time.Sleep(20 * time.Millisecond)
	var renewed jobs.Job
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/lease", job.ID),
		map[string]any{"worker_id": "w1", "lease_seconds": 60}, &renewed); code != http.StatusOK {
		t.Fatalf("renew: %d", code)
	}
	if !renewed.LeaseUntil.After(firstLease) {
		t.Errorf("lease_until = %v, want later than %v", renewed.LeaseUntil, firstLease)
	}

	var er map[string]string
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/lease", job.ID),
		map[string]any{"worker_id": "w2", "lease_seconds": 60}, &er); code != http.StatusConflict {
		t.Errorf("renew another worker's lease = %d, want 409", code)
	}
	if code := h.do(http.MethodPost, "/jobs/999999/lease",
		map[string]any{"worker_id": "w1"}, &er); code != http.StatusNotFound {
		t.Errorf("renew a nonexistent job = %d, want 404", code)
	}
}

// Catches: the core failure-recovery guarantee. A worker that dies without
// reporting must not strand its job forever.
func TestLeaseExpiryRequeuesJob(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("dying-worker")
	job := h.createJob("echo hi", 3)

	var claimed jobs.Job
	if code := h.claim("dying-worker", 0.001, &claimed); code != http.StatusOK {
		t.Fatalf("claim: %d", code)
	}
	// The worker now "crashes": it simply never reports and never renews.
	time.Sleep(50 * time.Millisecond)

	// Before the sweep the job is still RUNNING -- expiry alone changes nothing.
	if s := h.getJob(job.ID).Status; s != jobs.StatusRunning {
		t.Fatalf("status before reaping = %s, want RUNNING", s)
	}

	reaped, err := h.store.ReapExpiredLeases(context.Background(), 10)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].ID != job.ID {
		t.Fatalf("reaped = %+v, want exactly job %d", reaped, job.ID)
	}

	after := h.getJob(job.ID)
	if after.Status != jobs.StatusPending {
		t.Errorf("status = %s, want PENDING (retries remain)", after.Status)
	}
	if after.WorkerID != nil {
		t.Errorf("worker_id = %v, want nil: the dead worker must be released", *after.WorkerID)
	}
	if after.LeaseUntil != nil {
		t.Error("lease_until must be cleared")
	}
	if after.LastError == nil || *after.LastError == "" {
		t.Error("a reaped job must record why it was requeued")
	}
	if after.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: the crashed attempt still counts", after.Attempts)
	}

	// Another worker can now pick it up.
	h.registerWorker("healthy-worker")
	var reclaimed jobs.Job
	if code := h.claimWithin("healthy-worker", 3*time.Second, &reclaimed); code != http.StatusOK {
		t.Fatalf("reclaim = %d, want the requeued job to be claimable", code)
	}
	if reclaimed.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", reclaimed.Attempts)
	}
}

// Catches: a crashed worker's job being retried forever because lease expiry
// ignores max_attempts.
func TestLeaseExpiryRespectsMaxAttempts(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("echo hi", 1) // one attempt allowed

	var claimed jobs.Job
	h.claim("w1", 0.001, &claimed)
	time.Sleep(50 * time.Millisecond)

	reaped, err := h.store.ReapExpiredLeases(context.Background(), 10)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].Status != jobs.StatusFailed {
		t.Fatalf("reaped = %+v, want the job to land in FAILED", reaped)
	}
	if s := h.getJob(job.ID).Status; s != jobs.StatusFailed {
		t.Errorf("status = %s, want FAILED: an exhausted job must not be retried again", s)
	}
}

// Catches: the reaper touching healthy jobs whose leases are still valid, which
// would cause duplicate execution of every long-running job.
func TestReaperLeavesHealthyLeasesAlone(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("sleep 10", 3)

	var claimed jobs.Job
	h.claim("w1", 60, &claimed)

	reaped, err := h.store.ReapExpiredLeases(context.Background(), 10)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("reaped %+v, want none: this lease is still valid", reaped)
	}
	after := h.getJob(job.ID)
	if after.Status != jobs.StatusRunning || after.WorkerID == nil || *after.WorkerID != "w1" {
		t.Errorf("job = %s/%v, want it untouched and RUNNING under w1", after.Status, after.WorkerID)
	}
}

// Catches: a scheduled retry becoming claimable before its backoff elapses,
// which would turn a permanently-failing job into a hot retry loop that burns a
// worker and floods the database.
func TestBackoffDelaysRetry(t *testing.T) {
	h := newHarnessWithBackoff(t, jobs.BackoffConfig{Base: 400 * time.Millisecond, Max: time.Second})
	h.registerWorker("w1")
	job := h.createJob("exit 1", 3)

	var claimed jobs.Job
	h.claim("w1", 30, &claimed)
	var after jobs.Job
	h.failJob(job.ID, "w1", "boom", &after)
	if after.Status != jobs.StatusPending {
		t.Fatalf("status = %s, want PENDING", after.Status)
	}

	// Immediately after the failure the job must NOT be claimable.
	var er map[string]string
	if code := h.claim("w1", 30, &er); code != http.StatusNotFound {
		t.Fatalf("claim during backoff = %d, want 404: the retry delay was not applied", code)
	}

	// After the backoff elapses it must become claimable again.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var again jobs.Job
		if code := h.claim("w1", 30, &again); code == http.StatusOK {
			if again.Attempts != 2 {
				t.Errorf("attempts = %d, want 2", again.Attempts)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("job never became claimable after its backoff elapsed")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
