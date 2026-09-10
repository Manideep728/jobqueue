package tests

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
	"github.com/manideep7286/queue/internal/worker"
)

// TestConcurrentClaimSingleJob is the headline correctness test: many workers
// race for one job, and exactly one may win.
//
// Catches: the classic read-then-write race. Without FOR UPDATE (or with a
// naive "SELECT a pending job, then UPDATE it" in two statements), several
// workers read the same PENDING row and all mark it RUNNING -- the job then runs
// N times. For a job like "charge the customer" that is a production incident.
//
// Run with -race to also catch data races in the client and handler paths.
func TestConcurrentClaimSingleJob(t *testing.T) {
	h := newHarness(t)
	const contenders = 50

	for i := 0; i < contenders; i++ {
		h.registerWorker(fmt.Sprintf("w%d", i))
	}
	job := h.createJob("echo only-once", 5)

	var (
		mu        sync.Mutex
		winners   []string
		successes int
	)
	// A shared start signal makes the goroutines pile onto the claim at the same
	// instant instead of trickling in as they are scheduled.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			workerID := fmt.Sprintf("w%d", n)
			<-start
			var claimed jobs.Job
			if code := h.claim(workerID, 30, &claimed); code == http.StatusOK {
				mu.Lock()
				successes++
				winners = append(winners, workerID)
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Fatalf("%d workers claimed the same job (%v); exactly 1 may win", successes, winners)
	}

	final := h.getJob(job.ID)
	if final.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: only one claim may consume an attempt", final.Attempts)
	}
	if final.WorkerID == nil || *final.WorkerID != winners[0] {
		t.Errorf("worker_id = %v, want %s", final.WorkerID, winners[0])
	}
}

// TestConcurrentClaimManyJobs checks the same property at scale: N jobs handed
// to M concurrent workers must be distributed, with every job claimed exactly
// once and none lost.
//
// Catches: a claim query that returns the same row to two workers under load,
// and one that deadlocks or starves (SKIP LOCKED is what prevents both).
func TestConcurrentClaimManyJobs(t *testing.T) {
	h := newHarness(t)
	const (
		jobCount    = 50
		workerCount = 10
	)
	for i := 0; i < workerCount; i++ {
		h.registerWorker(fmt.Sprintf("w%d", i))
	}
	for i := 0; i < jobCount; i++ {
		h.createJob(fmt.Sprintf("echo job-%d", i), 3)
	}

	var (
		mu       sync.Mutex
		claimsOf = map[int64][]string{} // job id -> workers that claimed it
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			workerID := fmt.Sprintf("w%d", n)
			<-start
			// Keep claiming until the queue is empty.
			for {
				var claimed jobs.Job
				if code := h.claim(workerID, 60, &claimed); code != http.StatusOK {
					return
				}
				mu.Lock()
				claimsOf[claimed.ID] = append(claimsOf[claimed.ID], workerID)
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if len(claimsOf) != jobCount {
		t.Errorf("%d distinct jobs claimed, want %d: some jobs were never handed out", len(claimsOf), jobCount)
	}
	for id, owners := range claimsOf {
		if len(owners) != 1 {
			t.Errorf("job %d was claimed %d times by %v; want exactly once", id, len(owners), owners)
		}
	}

	// The database must agree: nothing left pending, every job attempted once.
	var stats struct {
		Jobs map[string]int64 `json:"jobs"`
	}
	h.do(http.MethodGet, "/stats", nil, &stats)
	if stats.Jobs["RUNNING"] != jobCount || stats.Jobs["PENDING"] != 0 {
		t.Errorf("stats = %v, want %d RUNNING and 0 PENDING", stats.Jobs, jobCount)
	}

	// And the work must actually be spread across workers, not hoarded by one.
	used := map[string]bool{}
	for _, owners := range claimsOf {
		used[owners[0]] = true
	}
	if len(used) < 2 {
		t.Errorf("only %d worker(s) got any work; jobs are not being distributed", len(used))
	}
}

// Catches: two concurrent completions both incrementing the worker's counter, or
// both being accepted so the second silently overwrites the first result.
func TestConcurrentCompletionOfSameJob(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("echo hi", 3)
	var claimed jobs.Job
	h.claim("w1", 60, &claimed)

	const attempts = 10
	var (
		mu   sync.Mutex
		ok   int
		wg   sync.WaitGroup
		gate = make(chan struct{})
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			if code := h.completeJob(job.ID, "w1", nil); code == http.StatusOK {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	close(gate)
	wg.Wait()

	// All of them may return 200 -- duplicates are treated as idempotent replays.
	// What must not happen is the job being counted more than once.
	if ok == 0 {
		t.Fatal("no completion succeeded")
	}
	var list struct {
		Workers []jobs.Worker `json:"workers"`
	}
	h.do(http.MethodGet, "/workers", nil, &list)
	if list.Workers[0].JobsProcessed != 1 {
		t.Errorf("jobs_processed = %d, want 1: concurrent duplicate completions were double-counted",
			list.Workers[0].JobsProcessed)
	}
	if s := h.getJob(job.ID).Status; s != jobs.StatusCompleted {
		t.Errorf("status = %s, want COMPLETED", s)
	}
}

// Catches: two server instances reaping the same expired lease and both
// requeuing it, which would produce two PENDING copies of one attempt.
func TestConcurrentReapersDoNotDoubleRequeue(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	job := h.createJob("echo hi", 5)

	var claimed jobs.Job
	h.claim("w1", 0.001, &claimed)
	time.Sleep(50 * time.Millisecond)

	const reapers = 5
	var (
		mu     sync.Mutex
		total  int
		wg     sync.WaitGroup
		gate   = make(chan struct{})
		errsCh = make(chan error, reapers)
	)
	for i := 0; i < reapers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			reaped, err := h.store.ReapExpiredLeases(context.Background(), 10)
			if err != nil {
				errsCh <- err
				return
			}
			mu.Lock()
			total += len(reaped)
			mu.Unlock()
		}()
	}
	close(gate)
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("concurrent reap failed: %v", err)
	}

	if total != 1 {
		t.Errorf("the job was reaped %d times, want exactly 1", total)
	}
	after := h.getJob(job.ID)
	if after.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: reaping must not consume extra attempts", after.Attempts)
	}
	if after.Status != jobs.StatusPending {
		t.Errorf("status = %s, want PENDING", after.Status)
	}
}

// TestWorkerPoolEndToEnd runs real worker processes (in-process, but the same
// code the binary runs) against the real API and checks that a batch of jobs is
// fully processed and spread across the pool.
//
// Catches: any integration-level break between claim, execute and report -- a
// worker that claims but never reports, a report the server rejects, a retry
// that never becomes claimable again.
func TestWorkerPoolEndToEnd(t *testing.T) {
	h := newHarness(t)

	const (
		workerCount = 3
		goodJobs    = 15
		flakyJobs   = 3
	)
	for i := 0; i < goodJobs; i++ {
		h.createJob(fmt.Sprintf("echo processed-%d", i), 3)
	}
	// Jobs that always fail, to exercise the retry path under a live pool.
	for i := 0; i < flakyJobs; i++ {
		h.createJob("exit 3", 2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		w := worker.New(worker.Config{
			ServerURL:         h.URL,
			WorkerID:          fmt.Sprintf("pool-worker-%d", i),
			Hostname:          "test",
			Lease:             5 * time.Second,
			PollInterval:      20 * time.Millisecond,
			HeartbeatInterval: time.Second,
			JobTimeout:        10 * time.Second,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("worker exited with error: %v", err)
			}
		}()
	}

	// Wait for every job to reach a terminal state.
	deadline := time.Now().Add(45 * time.Second)
	var stats struct {
		Jobs map[string]int64 `json:"jobs"`
	}
	for {
		h.do(http.MethodGet, "/stats", nil, &stats)
		if stats.Jobs["COMPLETED"] == goodJobs && stats.Jobs["FAILED"] == flakyJobs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out with stats %v; want %d COMPLETED and %d FAILED",
				stats.Jobs, goodJobs, flakyJobs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	// Every successful job must have captured its output exactly once.
	var all struct {
		Jobs []jobs.Job `json:"jobs"`
	}
	h.do(http.MethodGet, "/jobs?limit=100", nil, &all)
	busy := map[string]int{}
	for _, j := range all.Jobs {
		if j.Status != jobs.StatusCompleted {
			continue
		}
		if j.ResultStdout == nil || *j.ResultStdout == "" {
			t.Errorf("job %d completed with no captured stdout", j.ID)
		}
		if j.Attempts != 1 {
			t.Errorf("job %d took %d attempts; a job that always succeeds must take 1", j.ID, j.Attempts)
		}
		if j.WorkerID != nil {
			busy[*j.WorkerID]++
		}
	}
	if len(busy) < 2 {
		t.Errorf("work landed on %d worker(s) (%v); it should be distributed across the pool", len(busy), busy)
	}

	// Failing jobs must have used all their attempts and no more.
	for _, j := range all.Jobs {
		if j.Status == jobs.StatusFailed && j.Attempts != j.MaxAttempts {
			t.Errorf("failed job %d used %d of %d attempts", j.ID, j.Attempts, j.MaxAttempts)
		}
	}
}

// Catches: a worker that keeps executing a job after losing its lease, which is
// exactly the duplicate execution leases exist to prevent.
func TestWorkerAbandonsJobWhenLeaseIsLost(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("thief")

	// A long job with a lease short enough that renewal matters.
	h.createJob("sleep 20", 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := worker.New(worker.Config{
		ServerURL:         h.URL,
		WorkerID:          "slow-worker",
		Hostname:          "test",
		Lease:             time.Second, // renews every ~333ms
		PollInterval:      20 * time.Millisecond,
		HeartbeatInterval: time.Second,
		JobTimeout:        30 * time.Second,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()

	// Wait for the worker to pick the job up.
	deadline := time.Now().Add(10 * time.Second)
	for {
		j := h.getJob(1)
		if j.Status == jobs.StatusRunning && j.WorkerID != nil && *j.WorkerID == "slow-worker" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker never claimed the job")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Simulate the lease having lapsed and the job being reassigned: force it
	// back to PENDING and let another worker take it.
	if _, err := testDB.Exec(`UPDATE jobs SET status='PENDING', worker_id=NULL,
		lease_until=NULL, available_at=now() WHERE id=1`); err != nil {
		t.Fatalf("simulate reassignment: %v", err)
	}
	var stolen jobs.Job
	if code := h.claimWithin("thief", 5*time.Second, &stolen); code != http.StatusOK {
		t.Fatalf("thief claim = %d", code)
	}

	// The original worker's next renewal must get a 409 and make it give up,
	// which shows up as it going back to claiming (its current job released).
	deadline = time.Now().Add(10 * time.Second)
	for {
		j := h.getJob(1)
		// The job must still belong to the thief -- the abandoning worker must
		// never report on it.
		if j.WorkerID != nil && *j.WorkerID != "thief" {
			t.Fatalf("job owner = %s, want thief: the old worker reported on a job it had lost", *j.WorkerID)
		}
		if j.Status == jobs.StatusRunning && j.WorkerID != nil && *j.WorkerID == "thief" {
			// Give the losing worker time to notice and abandon.
			time.Sleep(1500 * time.Millisecond)
			final := h.getJob(1)
			if final.Status != jobs.StatusRunning || final.WorkerID == nil || *final.WorkerID != "thief" {
				t.Fatalf("job = %s/%v, want it still RUNNING under thief", final.Status, final.WorkerID)
			}
			cancel()
			<-done
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the reassignment to settle")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
