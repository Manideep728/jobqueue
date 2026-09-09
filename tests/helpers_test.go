// Package tests holds integration tests that exercise the real HTTP API against
// a real PostgreSQL database.
//
// They are integration tests on purpose: the interesting behavior of this
// project -- atomic claiming, lease expiry, retry scheduling -- lives in SQL.
// A test with a mocked store would pass while the actual queue handed the same
// job to two workers, which is precisely the bug worth catching.
//
// Run them with:
//
//	TEST_DATABASE_URL='postgres://queue:queue@localhost:55432/queue?sslmode=disable' go test -race ./tests/
//
// Without TEST_DATABASE_URL the whole package skips, so `go test ./...` still
// works on a machine with no database.
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/manideep7286/queue/internal/api"
	"github.com/manideep7286/queue/internal/database"
	"github.com/manideep7286/queue/internal/jobs"
)

// testDB is opened once for the package. Opening a pool per test would be slow
// and would hide connection leaks behind a fresh pool every time.
var testDB *sql.DB

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "TEST_DATABASE_URL not set; skipping integration tests")
		os.Exit(0)
	}
	// Keep test output about the tests, not about every HTTP request.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx := context.Background()
	db, err := database.WaitForDB(ctx, database.DefaultConfig(url), 30*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to TEST_DATABASE_URL:", err)
		os.Exit(1)
	}
	if err := database.Migrate(ctx, db); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	testDB = db
	code := m.Run()
	db.Close()
	os.Exit(code)
}

// harness is one isolated test environment: a clean database, a store and an
// in-process HTTP server.
type harness struct {
	t      *testing.T
	store  *jobs.Store
	server *httptest.Server
	URL    string
}

// newHarness truncates the tables and starts a server.
//
// Truncating before each test rather than after means a failing test leaves its
// rows behind for inspection. RESTART IDENTITY keeps ids predictable across
// tests, so a failure message naming "job 1" always means this test's job 1.
func newHarness(t *testing.T, opts ...func(*api.Config)) *harness {
	t.Helper()
	if _, err := testDB.Exec(`TRUNCATE jobs, workers RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	cfg := testAPIConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	store := jobs.NewStore(testDB, jobs.BackoffConfig{Base: 10 * time.Millisecond, Max: time.Second})
	srv := httptest.NewServer(api.New(store, cfg))
	t.Cleanup(srv.Close)

	return &harness{t: t, store: store, server: srv, URL: srv.URL}
}

// newHarnessWithBackoff is newHarness with a specific retry policy, for tests
// that assert on retry timing.
func newHarnessWithBackoff(t *testing.T, b jobs.BackoffConfig) *harness {
	t.Helper()
	h := newHarness(t)
	store := jobs.NewStore(testDB, b)
	srv := httptest.NewServer(api.New(store, testAPIConfig()))
	t.Cleanup(srv.Close)
	h.store, h.server, h.URL = store, srv, srv.URL
	return h
}

// do performs an API call and decodes the JSON body, returning the status code.
func (h *harness) do(method, path string, body any, out any) int {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal request: %v", err)
		}
		reader = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, h.URL+path, reader)
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.server.Client().Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			h.t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode
}

// doRaw is for malformed-body tests, where the request is not valid JSON.
func (h *harness) doRaw(method, path, body string) (int, string) {
	h.t.Helper()
	req, err := http.NewRequest(method, h.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.server.Client().Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// claimWithin retries a claim until it succeeds or the deadline passes.
//
// Needed because a requeued job is only claimable once its retry backoff has
// elapsed; claiming instantly would test the backoff rather than the thing the
// caller cares about.
func (h *harness) claimWithin(workerID string, within time.Duration, out any) int {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for {
		code := h.claim(workerID, 30, out)
		if code == http.StatusOK || time.Now().After(deadline) {
			return code
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// registerWorker registers a worker and fails the test if it cannot.
func (h *harness) registerWorker(id string) {
	h.t.Helper()
	if code := h.do(http.MethodPost, "/workers/register",
		map[string]string{"worker_id": id, "hostname": "test"}, nil); code != http.StatusCreated {
		h.t.Fatalf("register worker %s: status %d", id, code)
	}
}

// createJob submits a job and returns it.
func (h *harness) createJob(payload string, maxAttempts int) jobs.Job {
	h.t.Helper()
	var job jobs.Job
	code := h.do(http.MethodPost, "/jobs", map[string]any{
		"type": "shell", "payload": payload, "max_attempts": maxAttempts,
	}, &job)
	if code != http.StatusCreated {
		h.t.Fatalf("create job: status %d", code)
	}
	return job
}

// getJob reads a job back through the API.
func (h *harness) getJob(id int64) jobs.Job {
	h.t.Helper()
	var job jobs.Job
	if code := h.do(http.MethodGet, fmt.Sprintf("/jobs/%d", id), nil, &job); code != http.StatusOK {
		h.t.Fatalf("get job %d: status %d", id, code)
	}
	return job
}

// claim asks for a job on behalf of workerID, returning the status code.
func (h *harness) claim(workerID string, leaseSeconds float64, out any) int {
	h.t.Helper()
	return h.do(http.MethodPost, "/jobs/claim", map[string]any{
		"worker_id": workerID, "lease_seconds": leaseSeconds,
	}, out)
}

// testAPIConfig allows very short leases, so lease-expiry tests do not have to
// sleep for 30 seconds to observe a crash being recovered.
func testAPIConfig() api.Config {
	cfg := api.DefaultConfig()
	cfg.MinLease = time.Millisecond
	cfg.DefaultLease = 30 * time.Second
	return cfg
}

func ptrInt(v int) *int { return &v }
