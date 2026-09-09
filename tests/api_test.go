package tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

// Catches: a created job not starting PENDING, not being immediately claimable,
// or silently ignoring the requested max_attempts.
func TestCreateJob(t *testing.T) {
	h := newHarness(t)
	job := h.createJob("echo hello", 5)

	if job.ID == 0 {
		t.Error("created job has no id")
	}
	if job.Status != jobs.StatusPending {
		t.Errorf("status = %s, want PENDING", job.Status)
	}
	if job.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", job.Attempts)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("max_attempts = %d, want 5", job.MaxAttempts)
	}
	if job.WorkerID != nil || job.LeaseUntil != nil {
		t.Error("a new job must not have a worker or a lease")
	}
	if job.AvailableAt.After(job.CreatedAt.Add(time.Second)) {
		t.Error("a new job must be immediately available")
	}
}

// Catches: omitting max_attempts being stored as 0, which would create a job no
// worker is ever allowed to attempt.
func TestCreateJobDefaultsMaxAttempts(t *testing.T) {
	h := newHarness(t)
	var job jobs.Job
	code := h.do(http.MethodPost, "/jobs", map[string]any{
		"type": "shell", "payload": "echo hi",
	}, &job)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", code)
	}
	if job.MaxAttempts != 3 {
		t.Errorf("max_attempts = %d, want the default 3", job.MaxAttempts)
	}
}

// Catches: invalid input reaching the database and surfacing as a 500 instead of
// a 400 that explains the problem.
func TestCreateJobValidation(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing type", map[string]any{"payload": "echo hi"}},
		{"unknown type", map[string]any{"type": "python", "payload": "print(1)"}},
		{"missing payload", map[string]any{"type": "shell"}},
		{"blank payload", map[string]any{"type": "shell", "payload": "   "}},
		{"negative attempts", map[string]any{"type": "shell", "payload": "x", "max_attempts": -1}},
		{"absurd attempts", map[string]any{"type": "shell", "payload": "x", "max_attempts": 99999}},
		{"oversized payload", map[string]any{"type": "shell", "payload": strings.Repeat("x", 70000)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var er map[string]string
			code := h.do(http.MethodPost, "/jobs", tc.body, &er)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %v)", code, er)
			}
			if er["code"] != "bad_request" || er["error"] == "" {
				t.Errorf("error response = %v, want a bad_request code and a message", er)
			}
		})
	}
}

// Catches: malformed JSON producing a 500 or an empty 200 rather than a 400.
func TestMalformedRequests(t *testing.T) {
	h := newHarness(t)
	cases := []struct{ name, body string }{
		{"not json", "this is not json"},
		{"empty body", ""},
		{"truncated json", `{"type":"shell",`},
		{"wrong field type", `{"type":"shell","payload":"x","max_attempts":"three"}`},
		{"unknown field", `{"type":"shell","payload":"x","attemtps":3}`},
		{"trailing content", `{"type":"shell","payload":"x"}{"extra":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := h.doRaw(http.MethodPost, "/jobs", tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %s)", code, body)
			}
			if !strings.Contains(body, `"code"`) {
				t.Errorf("body %s is not the JSON error shape", body)
			}
		})
	}
}

// Catches: an unknown job id returning 500 or an empty 200, and a non-numeric id
// panicking the handler.
func TestInvalidJobIDs(t *testing.T) {
	h := newHarness(t)
	cases := []struct {
		path string
		want int
	}{
		{"/jobs/999999", http.StatusNotFound},
		{"/jobs/abc", http.StatusBadRequest},
		{"/jobs/-1", http.StatusBadRequest},
		{"/jobs/0", http.StatusBadRequest},
		{"/jobs/9999999999999999999999", http.StatusBadRequest}, // overflows int64
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			var er map[string]string
			if code := h.do(http.MethodGet, tc.path, nil, &er); code != tc.want {
				t.Fatalf("GET %s = %d, want %d", tc.path, code, tc.want)
			}
		})
	}
}

// Catches: an unknown path or a wrong method returning HTML/plain text, breaking
// clients that always parse JSON.
func TestUnknownRouteReturnsJSON(t *testing.T) {
	h := newHarness(t)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/nope", http.StatusNotFound},
		{http.MethodDelete, "/jobs/1", http.StatusMethodNotAllowed},
		// Not 405: "GET /jobs/{id}" matches this path with id="claim", so the
		// request legitimately reaches the get-job handler and is rejected there
		// as an invalid id. Documented here so the precedence is not a surprise.
		{http.MethodGet, "/jobs/claim", http.StatusBadRequest},
	} {
		code, body := h.doRaw(tc.method, tc.path, "")
		if code != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, code, tc.want)
		}
		if !strings.Contains(body, `"code"`) {
			t.Errorf("%s %s body = %q, want the JSON error shape", tc.method, tc.path, body)
		}
	}
}

// Catches: the status filter being ignored (returning everything), or an invalid
// filter being passed through to SQL.
func TestListJobsFiltering(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	for i := 0; i < 3; i++ {
		h.createJob(fmt.Sprintf("echo %d", i), 3)
	}
	var claimed jobs.Job
	if code := h.claim("w1", 30, &claimed); code != http.StatusOK {
		t.Fatalf("claim: status %d", code)
	}

	var all struct {
		Jobs  []jobs.Job `json:"jobs"`
		Count int        `json:"count"`
	}
	h.do(http.MethodGet, "/jobs", nil, &all)
	if all.Count != 3 {
		t.Fatalf("unfiltered count = %d, want 3", all.Count)
	}

	var pending struct {
		Jobs  []jobs.Job `json:"jobs"`
		Count int        `json:"count"`
	}
	h.do(http.MethodGet, "/jobs?status=pending", nil, &pending)
	if pending.Count != 2 {
		t.Errorf("pending count = %d, want 2", pending.Count)
	}
	for _, j := range pending.Jobs {
		if j.Status != jobs.StatusPending {
			t.Errorf("job %d has status %s in a PENDING filter", j.ID, j.Status)
		}
	}

	var running struct {
		Count int `json:"count"`
	}
	h.do(http.MethodGet, "/jobs?status=RUNNING", nil, &running)
	if running.Count != 1 {
		t.Errorf("running count = %d, want 1", running.Count)
	}

	var er map[string]string
	if code := h.do(http.MethodGet, "/jobs?status=BOGUS", nil, &er); code != http.StatusBadRequest {
		t.Errorf("invalid status filter = %d, want 400", code)
	}
	if code := h.do(http.MethodGet, "/jobs?limit=abc", nil, &er); code != http.StatusBadRequest {
		t.Errorf("invalid limit = %d, want 400", code)
	}
}

// Catches: cancel being allowed on a RUNNING job (a promise the queue cannot
// keep, since it cannot kill a command another process already started).
func TestCancelJob(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")

	pending := h.createJob("echo cancel-me", 3)
	var canceled jobs.Job
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/cancel", pending.ID), map[string]any{}, &canceled); code != http.StatusOK {
		t.Fatalf("cancel pending job: status %d", code)
	}
	if canceled.Status != jobs.StatusCanceled {
		t.Errorf("status = %s, want CANCELED", canceled.Status)
	}

	// Canceling again is a no-op, not an error: the caller's intent already holds.
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/cancel", pending.ID), map[string]any{}, nil); code != http.StatusOK {
		t.Errorf("second cancel: status %d, want 200 (idempotent)", code)
	}

	// A canceled job must never be handed to a worker.
	var claimed jobs.Job
	if code := h.claim("w1", 30, &claimed); code != http.StatusNotFound {
		t.Errorf("claim after cancel = %d, want 404: a canceled job must not be claimable", code)
	}

	// A RUNNING job cannot be canceled.
	running := h.createJob("sleep 1", 3)
	if code := h.claim("w1", 30, &claimed); code != http.StatusOK {
		t.Fatalf("claim: status %d", code)
	}
	var er map[string]string
	if code := h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/cancel", running.ID), map[string]any{}, &er); code != http.StatusConflict {
		t.Errorf("cancel running job = %d, want 409", code)
	}

	if code := h.do(http.MethodPost, "/jobs/999999/cancel", map[string]any{}, &er); code != http.StatusNotFound {
		t.Errorf("cancel unknown job = %d, want 404", code)
	}
}

// Catches: workers being able to claim without registering, which would make the
// workers table (and heartbeat liveness) describe only part of the fleet.
func TestClaimRequiresRegistration(t *testing.T) {
	h := newHarness(t)
	h.createJob("echo hi", 3)

	var er map[string]string
	if code := h.claim("ghost-worker", 30, &er); code != http.StatusNotFound {
		t.Fatalf("claim by unregistered worker = %d, want 404", code)
	}

	// Crucially, the failed claim must not have consumed the job.
	job := h.getJob(1)
	if job.Status != jobs.StatusPending {
		t.Errorf("job status = %s, want PENDING: a rejected claim must roll back", job.Status)
	}
	if job.Attempts != 0 {
		t.Errorf("attempts = %d, want 0: a rejected claim must not burn an attempt", job.Attempts)
	}
}

// Catches: a claim without a worker_id being accepted and leaving a job owned by
// nobody, permanently stuck until its lease expires.
func TestClaimValidation(t *testing.T) {
	h := newHarness(t)
	h.createJob("echo hi", 3)
	var er map[string]string
	for _, body := range []map[string]any{{}, {"worker_id": ""}, {"worker_id": "   "}} {
		if code := h.do(http.MethodPost, "/jobs/claim", body, &er); code != http.StatusBadRequest {
			t.Errorf("claim %v = %d, want 400", body, code)
		}
	}
}

// Catches: an empty queue being reported as a server error rather than the
// ordinary idle case, which would fill worker logs with false alarms.
func TestClaimEmptyQueue(t *testing.T) {
	h := newHarness(t)
	h.registerWorker("w1")
	var er map[string]string
	code := h.claim("w1", 30, &er)
	if code != http.StatusNotFound {
		t.Fatalf("claim on empty queue = %d, want 404", code)
	}
	if er["code"] != "no_jobs_available" {
		t.Errorf("code = %q, want no_jobs_available so workers can tell it apart from a real 404", er["code"])
	}
}

// Catches: heartbeats from an unknown worker being accepted (hiding a reset
// database) or worker counters not being tracked.
func TestWorkerRegistrationAndHeartbeat(t *testing.T) {
	h := newHarness(t)

	var w jobs.Worker
	if code := h.do(http.MethodPost, "/workers/register",
		map[string]string{"worker_id": "w1", "hostname": "testhost"}, &w); code != http.StatusCreated {
		t.Fatalf("register: status %d", code)
	}
	if w.ID != "w1" || w.Hostname != "testhost" || !w.Alive {
		t.Errorf("registered worker = %+v, want id w1, hostname testhost, alive", w)
	}

	// Re-registering must succeed, so a restarted worker keeps its identity.
	if code := h.do(http.MethodPost, "/workers/register",
		map[string]string{"worker_id": "w1", "hostname": "testhost"}, nil); code != http.StatusCreated {
		t.Errorf("re-register: status %d, want 201", code)
	}

	if code := h.do(http.MethodPost, "/workers/w1/heartbeat", nil, nil); code != http.StatusOK {
		t.Errorf("heartbeat: status %d", code)
	}
	if code := h.do(http.MethodPost, "/workers/nobody/heartbeat", nil, nil); code != http.StatusNotFound {
		t.Errorf("heartbeat from unknown worker = %d, want 404 so it knows to re-register", code)
	}

	var er map[string]string
	if code := h.do(http.MethodPost, "/workers/register", map[string]string{"worker_id": ""}, &er); code != http.StatusBadRequest {
		t.Errorf("register with empty id = %d, want 400", code)
	}

	// jobs_processed must count finished work.
	job := h.createJob("echo hi", 3)
	var claimed jobs.Job
	h.claim("w1", 30, &claimed)
	h.do(http.MethodPost, fmt.Sprintf("/jobs/%d/complete", job.ID),
		map[string]any{"worker_id": "w1", "exit_code": ptrInt(0)}, nil)

	var list struct {
		Workers []jobs.Worker `json:"workers"`
	}
	h.do(http.MethodGet, "/workers", nil, &list)
	if len(list.Workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(list.Workers))
	}
	if list.Workers[0].JobsProcessed != 1 {
		t.Errorf("jobs_processed = %d, want 1", list.Workers[0].JobsProcessed)
	}
	if list.Workers[0].CurrentJobID != nil {
		t.Error("current_job_id must be cleared once the job finishes")
	}
}

// Catches: health reporting OK without checking the database, which would let an
// orchestrator route traffic to a server that cannot serve a single request.
func TestHealthAndStats(t *testing.T) {
	h := newHarness(t)
	var health map[string]string
	if code := h.do(http.MethodGet, "/healthz", nil, &health); code != http.StatusOK {
		t.Fatalf("healthz = %d", code)
	}
	if health["status"] != "ok" {
		t.Errorf("health = %v", health)
	}

	h.createJob("echo hi", 3)
	var stats struct {
		Jobs map[string]int64 `json:"jobs"`
	}
	h.do(http.MethodGet, "/stats", nil, &stats)
	if stats.Jobs["PENDING"] != 1 {
		t.Errorf("PENDING = %d, want 1", stats.Jobs["PENDING"])
	}
	// Every status must be present, including the zeros: a missing key reads as
	// "unknown" rather than "none".
	for _, s := range []string{"PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELED"} {
		if _, ok := stats.Jobs[s]; !ok {
			t.Errorf("stats is missing the %s key", s)
		}
	}
}
