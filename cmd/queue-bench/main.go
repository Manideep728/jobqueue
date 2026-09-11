// Command queue-bench measures the throughput and latency figures published in
// the README, against a running queue stack.
//
// It is a load generator rather than a set of testing.B benchmarks on purpose.
// testing.B scales b.N until an in-process function takes about a second and
// reports ns/op; the quantity of interest here is wall-clock throughput of the
// whole pipeline -- submit, claim, fork/exec, report -- across however many
// worker processes are running. A fixed job count measures exactly that, and
// b.N's auto-scaling would fight the queue's own pacing.
//
// Usage:
//
//	docker compose up -d --scale worker=5
//	go run ./cmd/queue-bench --jobs 300 --concurrency 16
//
// Every number it prints is measured. Nothing is extrapolated.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	server := flag.String("server", envOr("QUEUE_SERVER", "http://localhost:8080"), "queue server URL")
	total := flag.Int("jobs", 300, "jobs to submit for the throughput phase")
	concurrency := flag.Int("concurrency", 16, "concurrent submit connections")
	samples := flag.Int("latency-samples", 20, "single jobs to time against an idle pool")
	payload := flag.String("payload", "true", "job payload; the default is a no-op so the queue is measured, not the work")
	drainTimeout := flag.Duration("drain-timeout", 2*time.Minute, "give up if the queue has not drained in this long")
	flag.Parse()

	b := &bench{server: *server, token: os.Getenv("QUEUE_AUTH_TOKEN"), payload: *payload}
	// One connection per submitter. net/http keeps only 2 idle connections per
	// host by default, so without this the submit phase would spend its time
	// opening and closing sockets and understate its own result.
	b.http = &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency * 2},
	}

	if err := b.run(*total, *concurrency, *samples, *drainTimeout); err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type bench struct {
	server  string
	token   string
	payload string
	http    *http.Client
}

// result is one row of the printed table.
type result struct {
	metric string
	value  string
}

func (b *bench) run(total, concurrency, samples int, drainTimeout time.Duration) error {
	workers, err := b.aliveWorkers()
	if err != nil {
		return fmt.Errorf("cannot reach %s: %w", b.server, err)
	}
	if workers == 0 {
		return fmt.Errorf("no live workers registered at %s; start some with: docker compose up -d --scale worker=5", b.server)
	}
	fmt.Printf("server %s, %d live worker(s), %d jobs, %d submit connections, payload %q\n\n",
		b.server, workers, total, concurrency, b.payload)

	// Drain progress is measured as a delta in the terminal-job count, which is
	// only sound if nothing else is in flight: jobs left over from an earlier
	// aborted run would finish during this one and count as ours, reporting a
	// throughput the run never achieved.
	if err := b.requireEmptyQueue(); err != nil {
		return err
	}
	baseline, err := b.terminalCount()
	if err != nil {
		return err
	}

	var rows []result
	start := time.Now()
	submitted, submitRate, err := b.submitPhase(total, concurrency)
	if err != nil {
		return err
	}
	rows = append(rows, result{
		fmt.Sprintf("Submit throughput (%d connections)", concurrency),
		fmt.Sprintf("%.0f jobs/s", submitRate),
	})

	endToEnd, execMS, err := b.drainPhase(baseline, submitted, start, drainTimeout)
	if err != nil {
		return err
	}
	rows = append(rows,
		result{"End-to-end throughput", fmt.Sprintf("%.0f jobs/s", endToEnd)},
		result{"Job execution time (median)", formatExecMS(execMS)},
	)

	if samples > 0 {
		lo, med, hi, err := b.latencyPhase(samples)
		if err != nil {
			return err
		}
		rows = append(rows, result{
			fmt.Sprintf("Idle-pool latency, submit to terminal (n=%d)", samples),
			fmt.Sprintf("%d ms min / %d ms median / %d ms max", lo, med, hi),
		})
	}

	printTable(rows)
	return nil
}

// submitPhase fires total jobs at POST /jobs from concurrency goroutines and
// reports how many landed and at what rate.
func (b *bench) submitPhase(total, concurrency int) (int, float64, error) {
	fmt.Printf("submitting %d jobs...\n", total)

	work := make(chan int)
	var (
		mu     sync.Mutex
		ok     int
		failed error
	)
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range work {
				var job struct{ ID int64 }
				err := b.post("/jobs", map[string]any{"type": "shell", "payload": b.payload}, &job)
				mu.Lock()
				if err != nil {
					if failed == nil {
						failed = err
					}
				} else {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < total; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	elapsed := time.Since(start)

	if failed != nil {
		return 0, 0, fmt.Errorf("submit: %w", failed)
	}
	rate := float64(ok) / elapsed.Seconds()
	fmt.Printf("  %d submitted in %s -> %.0f jobs/s\n", ok, elapsed.Round(time.Millisecond), rate)
	return ok, rate, nil
}

// drainPhase waits until every submitted job has reached a terminal state, then
// reports the pipeline rate and the median executor runtime the workers reported.
// The clock starts at the caller's start, not here, so submission counts against
// the rate -- a producer cannot submit and execute at the same time.
func (b *bench) drainPhase(baseline, submitted int, start time.Time, timeout time.Duration) (float64, int64, error) {
	fmt.Printf("waiting for %d jobs to finish...\n", submitted)
	deadline := time.Now().Add(timeout)
	lastCheck := time.Now()

	for {
		done, err := b.terminalCount()
		if err != nil {
			return 0, 0, err
		}
		if done-baseline >= submitted {
			break
		}
		if time.Now().After(deadline) {
			return 0, 0, fmt.Errorf("only %d/%d jobs finished within %s", done-baseline, submitted, timeout)
		}
		// A worker that was alive at startup can stop heartbeating mid-run. Without
		// this the run would sit out the whole drain timeout before saying so.
		if time.Since(lastCheck) > 5*time.Second {
			lastCheck = time.Now()
			alive, err := b.aliveWorkers()
			if err != nil {
				return 0, 0, err
			}
			if alive == 0 {
				return 0, 0, fmt.Errorf("every worker stopped heartbeating with %d/%d jobs finished; %d are left in the queue",
					done-baseline, submitted, submitted-(done-baseline))
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(start)
	rate := float64(submitted) / elapsed.Seconds()
	fmt.Printf("  drained in %s -> %.0f jobs/s end to end\n", elapsed.Round(time.Millisecond), rate)

	execMS, err := b.medianDurationMS(submitted)
	if err != nil {
		return 0, 0, err
	}
	fmt.Printf("  median reported execution time %d ms\n", execMS)
	return rate, execMS, nil
}

// latencyPhase times single jobs against an otherwise idle pool. It sleeps
// between samples so each one lands on workers that are genuinely idle -- that
// is what makes the poll-interval wait visible instead of averaged away.
func (b *bench) latencyPhase(samples int) (int64, int64, int64, error) {
	fmt.Printf("timing %d single jobs against an idle pool...\n", samples)
	ms := make([]int64, 0, samples)
	for i := 0; i < samples; i++ {
		if i > 0 {
			time.Sleep(1500 * time.Millisecond)
		}
		start := time.Now()
		var job struct{ ID int64 }
		if err := b.post("/jobs", map[string]any{"type": "shell", "payload": b.payload}, &job); err != nil {
			return 0, 0, 0, fmt.Errorf("latency submit: %w", err)
		}
		if err := b.awaitTerminal(job.ID, 60*time.Second); err != nil {
			return 0, 0, 0, err
		}
		ms = append(ms, time.Since(start).Milliseconds())
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
	lo, med, hi := ms[0], ms[len(ms)/2], ms[len(ms)-1]
	fmt.Printf("  %d ms min / %d ms median / %d ms max\n", lo, med, hi)
	return lo, med, hi, nil
}

// formatExecMS renders the reported runtime. duration_ms is integer
// milliseconds, so a no-op payload legitimately reports 0 -- printing that bare
// would read as "not measured" rather than "faster than the unit".
func formatExecMS(ms int64) string {
	if ms == 0 {
		return "<1 ms (no-op payload; process spawn dominates)"
	}
	return fmt.Sprintf("%d ms", ms)
}

// awaitTerminal polls one job until it stops being PENDING or RUNNING.
func (b *bench) awaitTerminal(id int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var job struct {
			Status string `json:"status"`
		}
		if err := b.get(fmt.Sprintf("/jobs/%d", id), &job); err != nil {
			return err
		}
		switch job.Status {
		case "COMPLETED", "FAILED", "CANCELED":
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("job %d still %s after %s", id, job.Status, timeout)
		}
		// Poll fast: this interval is measurement error on every latency sample.
		time.Sleep(5 * time.Millisecond)
	}
}

type statsResponse struct {
	Jobs         map[string]int64 `json:"jobs"`
	WorkersAlive int              `json:"workers_alive"`
}

// terminalCount is how many jobs the queue has finished with, in any sense.
func (b *bench) terminalCount() (int, error) {
	var s statsResponse
	if err := b.get("/stats", &s); err != nil {
		return 0, err
	}
	return int(s.Jobs["COMPLETED"] + s.Jobs["FAILED"] + s.Jobs["CANCELED"]), nil
}

// requireEmptyQueue refuses to run while work is outstanding.
func (b *bench) requireEmptyQueue() error {
	var s statsResponse
	if err := b.get("/stats", &s); err != nil {
		return err
	}
	if backlog := s.Jobs["PENDING"] + s.Jobs["RUNNING"]; backlog > 0 {
		return fmt.Errorf("%d job(s) still pending or running; let the queue drain first, or reset it with: make down ARGS=-v && make up", backlog)
	}
	return nil
}

func (b *bench) aliveWorkers() (int, error) {
	var s statsResponse
	if err := b.get("/stats", &s); err != nil {
		return 0, err
	}
	return s.WorkersAlive, nil
}

// medianDurationMS is the median duration_ms the workers reported for the most
// recent n completed jobs -- the executor's own view, not counting queue wait.
func (b *bench) medianDurationMS(n int) (int64, error) {
	if n > 500 {
		n = 500 // the API's limit ceiling
	}
	var list struct {
		Jobs []struct {
			DurationMS *int64 `json:"duration_ms"`
		} `json:"jobs"`
	}
	if err := b.get(fmt.Sprintf("/jobs?status=completed&limit=%d", n), &list); err != nil {
		return 0, err
	}
	var ms []int64
	for _, j := range list.Jobs {
		if j.DurationMS != nil {
			ms = append(ms, *j.DurationMS)
		}
	}
	if len(ms) == 0 {
		return 0, fmt.Errorf("no completed job reported a duration")
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
	return ms[len(ms)/2], nil
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

func (b *bench) get(path string, out any) error        { return b.do("GET", path, nil, out) }
func (b *bench) post(path string, body, out any) error { return b.do("POST", path, body, out) }

func (b *bench) do(method, path string, body, out any) error {
	var buf *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(raw)
	} else {
		buf = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, b.server+path, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.token)
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("%s %s: %d %s: %s", method, path, resp.StatusCode, e.Code, e.Error)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// printTable prints the results as a Markdown table, so a run can be pasted
// into the README without anyone retyping -- and mistyping -- a number.
func printTable(rows []result) {
	width := 0
	for _, r := range rows {
		if len(r.metric) > width {
			width = len(r.metric)
		}
	}
	fmt.Printf("\n| %-*s | Result |\n", width, "Metric")
	fmt.Printf("|%s|--------|\n", bytes.Repeat([]byte("-"), width+2))
	for _, r := range rows {
		fmt.Printf("| %-*s | %s |\n", width, r.metric, r.value)
	}
}
