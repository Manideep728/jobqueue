package main

// Claim-contention mode.
//
// The pipeline mode measures the whole system, which means its numbers are
// bounded by whatever is slowest -- usually the workers' fork/exec, not the
// queue. This mode isolates the one operation that is actually load-bearing:
// handing a job to exactly one of N workers competing for it. Claimers here do
// nothing with what they claim, so the rate is the claim path and nothing else.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// claimLease is deliberately far longer than any run. Claimed jobs stay RUNNING
// for the whole benchmark, and the reaper requeues a job whose lease expires --
// so a short lease would let a job claimed at one level become PENDING again and
// be claimed a second time at a later level. That is correct queue behavior, but
// the duplicate check cannot tell it apart from a genuine double-dispatch, so
// the run must finish well inside the lease. 10 minutes is the server's MaxLease.
const claimLease = 600

// levelResult is one row of the contention curve.
type levelResult struct {
	claimers int
	claimed  int
	elapsed  time.Duration
	rate     float64
}

// emptyConfirmDelay is how long a claimer waits before believing the queue is
// empty. See claimUntilEmpty for why a single empty response is not conclusive.
const emptyConfirmDelay = 20 * time.Millisecond

// claimUntilEmpty claims in a tight loop until the queue is really drained.
//
// It takes a second look before giving up. SKIP LOCKED makes the claim return
// no rows when every remaining candidate row is locked by another claimer --
// not only when the table is actually empty. Near the tail of the backlog, when
// fewer jobs remain than there are claimers, a claimer can therefore be told
// "empty" while jobs are still PENDING. Quitting on that first answer would
// leave rows behind for the next level to inherit and would stop the clock
// early. One short pause is enough for the competing locks to commit.
func (b *bench) claimUntilEmpty(worker string) ([]claimRecord, error) {
	var out []claimRecord
	body := map[string]any{"worker_id": worker, "lease_seconds": claimLease}
	for {
		id, got, err := b.claimOne(body)
		if err != nil {
			return nil, err
		}
		if !got {
			time.Sleep(emptyConfirmDelay)
			if id, got, err = b.claimOne(body); err != nil {
				return nil, err
			}
			if !got {
				return out, nil
			}
		}
		out = append(out, claimRecord{jobID: id, worker: worker})
	}
}

// claimOne attempts a single claim. got is false when the server reported an
// empty queue, which is an ordinary end condition rather than an error.
func (b *bench) claimOne(body map[string]any) (int64, bool, error) {
	var job struct{ ID int64 }
	err := b.post("/jobs/claim", body, &job)
	if err == nil {
		return job.ID, true, nil
	}
	if hasCode(err, "no_jobs_available") {
		return 0, false, nil
	}
	return 0, false, err
}

// claimRecord is one successful claim: which job, and who got it. Keeping the
// claimer alongside the id is what lets a duplicate be reported as "job 41 went
// to both A and B" rather than just a count.
type claimRecord struct {
	jobID  int64
	worker string
}

// raceClaimers registers n claimers, turns them loose on the existing backlog,
// and times how long they take to drain it.
func (b *bench) raceClaimers(level int) (levelResult, []claimRecord, error) {
	workers := make([]string, level)
	for i := range workers {
		workers[i] = fmt.Sprintf("bench-claimer-%d-%d", level, i)
		body := map[string]any{"worker_id": workers[i], "hostname": "queue-bench"}
		if err := b.post("/workers/register", body, nil); err != nil {
			return levelResult{}, nil, fmt.Errorf("register %s: %w", workers[i], err)
		}
	}

	// Per-goroutine result slots, so no claimer ever touches another's slice and
	// the whole race needs no mutex. Each index is written by exactly one
	// goroutine and read only after Wait.
	claims := make([][]claimRecord, level)
	errs := make([]error, level)

	var wg sync.WaitGroup
	start := time.Now()
	for i := range level {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims[i], errs[i] = b.claimUntilEmpty(workers[i])
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []claimRecord
	for i := range level {
		if errs[i] != nil {
			return levelResult{}, nil, fmt.Errorf("claimer %s: %w", workers[i], errs[i])
		}
		all = append(all, claims[i]...)
	}

	res := levelResult{
		claimers: level,
		claimed:  len(all),
		elapsed:  elapsed,
		rate:     float64(len(all)) / elapsed.Seconds(),
	}
	fmt.Printf("  %2d claimer(s): %d claims in %s -> %.0f claims/s\n",
		level, res.claimed, elapsed.Round(time.Millisecond), res.rate)
	return res, all, nil
}

// parseLevels turns "1,4,16,64" into claimer counts.
func parseLevels(s string) ([]int, error) {
	var out []int
	for _, field := range strings.Split(s, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		n, err := strconv.Atoi(field)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("--claim-levels: %q is not a positive integer", field)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("--claim-levels is empty")
	}
	return out, nil
}

// seedBacklog fills the queue with PENDING jobs for one level to drain.
//
// max_attempts is 1 so that failing a job during cleanup is terminal. At the
// default the queue would do its job and schedule a retry, putting every job
// straight back into PENDING and leaving the queue dirtier than it started.
func (b *bench) seedBacklog(total, concurrency int) error {
	work := make(chan struct{})
	var (
		mu     sync.Mutex
		failed error
	)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := map[string]any{"type": "shell", "payload": b.payload, "max_attempts": 1}
			for range work {
				if err := b.post("/jobs", body, nil); err != nil {
					mu.Lock()
					if failed == nil {
						failed = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for range total {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()
	return failed
}

// runClaim measures claim throughput at each concurrency level in turn.
func (b *bench) runClaim(levels []int, seed, concurrency int) error {
	alive, err := b.foreignAliveWorkers()
	if err != nil {
		return fmt.Errorf("cannot reach %s: %w", b.server, err)
	}
	// The opposite precondition from pipeline mode, which requires live workers.
	// Here a live worker would claim jobs out from under the benchmark: the
	// measured rate would be an unknown fraction of the real one, and the jobs
	// it won would actually execute.
	if alive > 0 {
		return fmt.Errorf("%d worker(s) still heartbeating at %s; they would compete for the backlog. Stop them with: docker compose up -d --scale worker=0", alive, b.server)
	}
	if err := b.requireEmptyQueue(); err != nil {
		return err
	}
	fmt.Printf("server %s, no live workers, %d jobs seeded per level, levels %v, payload %q\n\n",
		b.server, seed, levels, b.payload)

	// seen maps every job id claimed during the whole run to the claimer that
	// got it, across all levels. A second appearance is a double dispatch.
	seen := make(map[int64]string, seed*len(levels))
	var (
		rows    []levelResult
		claimed []claimRecord
		dupes   int
	)

	for _, level := range levels {
		fmt.Printf("seeding %d jobs for %d claimer(s)...\n", seed, level)
		if err := b.seedBacklog(seed, concurrency); err != nil {
			return fmt.Errorf("seed: %w", err)
		}
		res, records, err := b.raceClaimers(level)
		if err != nil {
			return err
		}
		rows = append(rows, res)
		claimed = append(claimed, records...)
		for _, r := range records {
			if prev, ok := seen[r.jobID]; ok {
				dupes++
				fmt.Printf("  DOUBLE DISPATCH: job %d claimed by both %s and %s\n", r.jobID, prev, r.worker)
			}
			seen[r.jobID] = r.worker
		}
	}

	if err := b.cleanup(claimed, concurrency); err != nil {
		return err
	}
	printClaimTable(rows, len(seen), dupes)
	return nil
}

// benchWorkerPrefix marks the claimers this benchmark registers, so a later run
// can tell them apart from a real worker fleet.
const benchWorkerPrefix = "bench-claimer-"

// foreignAliveWorkers counts live workers that this benchmark did not create.
//
// Claiming stamps last_heartbeat (the store updates it in the same transaction
// that hands over the job), so our own claimers look alive for the heartbeat
// timeout after a run finishes. Counting those would make a second run inside
// a minute refuse to start, blaming a worker fleet that is not running.
func (b *bench) foreignAliveWorkers() (int, error) {
	var list struct {
		Workers []struct {
			ID    string `json:"id"`
			Alive bool   `json:"alive"`
		} `json:"workers"`
	}
	if err := b.get("/workers", &list); err != nil {
		return 0, err
	}
	n := 0
	for _, w := range list.Workers {
		if w.Alive && !strings.HasPrefix(w.ID, benchWorkerPrefix) {
			n++
		}
	}
	return n, nil
}

// cleanup returns the queue to the empty state it was found in. Every job this
// run claimed is still RUNNING under a ten-minute lease, and the claimers may
// have left a few PENDING. Skipping this would make the next run's
// requireEmptyQueue refuse to start, and would leave the reaper requeuing
// abandoned leases for ten minutes.
func (b *bench) cleanup(claimed []claimRecord, concurrency int) error {
	fmt.Printf("\ncleaning up %d claimed job(s)...\n", len(claimed))

	work := make(chan claimRecord)
	var (
		mu     sync.Mutex
		failed error
	)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				// Fenced on worker_id, so each job must be failed by the claimer
				// that holds it; max_attempts is 1, so this is terminal.
				body := map[string]any{"worker_id": r.worker, "error": "queue-bench cleanup"}
				if err := b.post(fmt.Sprintf("/jobs/%d/fail", r.jobID), body, nil); err != nil {
					mu.Lock()
					if failed == nil {
						failed = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, r := range claimed {
		work <- r
	}
	close(work)
	wg.Wait()
	if failed != nil {
		return fmt.Errorf("cleanup: %w", failed)
	}
	return b.cancelPending()
}

// cancelPending cancels whatever the claimers left behind, a page at a time
// until the queue reports none left.
func (b *bench) cancelPending() error {
	for {
		var list struct {
			Jobs []struct {
				ID int64 `json:"id"`
			} `json:"jobs"`
		}
		if err := b.get("/jobs?status=pending&limit=500", &list); err != nil {
			return err
		}
		if len(list.Jobs) == 0 {
			return nil
		}
		for _, j := range list.Jobs {
			if err := b.post(fmt.Sprintf("/jobs/%d/cancel", j.ID), nil, nil); err != nil {
				return fmt.Errorf("cancel leftover job %d: %w", j.ID, err)
			}
		}
	}
}

// printClaimTable renders the curve, with the correctness verdict as its last
// row: a claim rate means nothing without it.
func printClaimTable(rows []levelResult, unique, dupes int) {
	out := make([]result, 0, len(rows)+1)
	for _, r := range rows {
		out = append(out, result{
			fmt.Sprintf("Claim throughput, %d concurrent claimer(s)", r.claimers),
			fmt.Sprintf("%.0f claims/s", r.rate),
		})
	}
	out = append(out, result{"Double dispatch", claimVerdict(unique, dupes)})
	printTable(out)
}

// claimVerdict states the exactly-once result. A duplicate has to read as a
// failure: the whole point of the mode is that this line cannot be quietly
// reassuring when the queue dispatched a job twice.
func claimVerdict(unique, dupes int) string {
	if dupes > 0 {
		return fmt.Sprintf("FAILED: %d duplicate claim(s) across %d jobs", dupes, unique)
	}
	return fmt.Sprintf("0 of %d jobs claimed twice", unique)
}
