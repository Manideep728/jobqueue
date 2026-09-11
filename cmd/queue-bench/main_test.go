package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestBench points a bench at a stub server.
func newTestBench(t *testing.T, handler http.HandlerFunc) *bench {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &bench{server: srv.URL, http: srv.Client()}
}

// A null duration_ms is normal (a job that never ran), and counting it as zero
// would drag the reported median toward zero -- a made-up number.
func TestMedianDurationMSIgnoresNulls(t *testing.T) {
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Chosen so that counting the null as 0 would move the median to 10:
		// [10 30] -> 30, but [0 10 30] -> 10.
		_, _ = w.Write([]byte(`{"jobs":[{"duration_ms":10},{"duration_ms":null},{"duration_ms":30}],"count":3}`))
	})
	got, err := b.medianDurationMS(10)
	if err != nil {
		t.Fatalf("medianDurationMS: %v", err)
	}
	if got != 30 {
		t.Errorf("median = %d ms; want 30 (median of 10 and 30, ignoring the null)", got)
	}
}

// If every duration is null there is nothing to report, and reporting 0 would
// look like a measurement instead of the absence of one.
func TestMedianDurationMSErrorsWithNoData(t *testing.T) {
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"duration_ms":null}],"count":1}`))
	})
	if _, err := b.medianDurationMS(10); err == nil {
		t.Fatal("want an error when no job reported a duration, got nil")
	}
}

// The drain phase counts a delta in terminal jobs, so leftover work from an
// earlier run would be credited to this one. The preflight must refuse.
func TestRequireEmptyQueue(t *testing.T) {
	cases := map[string]struct {
		body      string
		wantError bool
	}{
		"empty":           {`{"jobs":{"PENDING":0,"RUNNING":0,"COMPLETED":9},"workers_alive":2}`, false},
		"pending backlog": {`{"jobs":{"PENDING":7,"RUNNING":0,"COMPLETED":0},"workers_alive":2}`, true},
		"running backlog": {`{"jobs":{"PENDING":0,"RUNNING":1,"COMPLETED":0},"workers_alive":2}`, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			})
			err := b.requireEmptyQueue()
			if tc.wantError && err == nil {
				t.Error("want an error for an outstanding backlog, got nil")
			}
			if !tc.wantError && err != nil {
				t.Errorf("want no error for an empty queue, got %v", err)
			}
		})
	}
}

// A no-op payload really does finish in under a millisecond; a bare "0 ms" reads
// as a missing measurement.
func TestFormatExecMS(t *testing.T) {
	if got := formatExecMS(0); !strings.HasPrefix(got, "<1 ms") {
		t.Errorf("formatExecMS(0) = %q; want it to start with \"<1 ms\"", got)
	}
	if got := formatExecMS(12); got != "12 ms" {
		t.Errorf("formatExecMS(12) = %q; want \"12 ms\"", got)
	}
}

// A non-2xx response must surface the server's message, not be silently decoded
// into a zero value that later looks like a real result.
func TestDoReportsServerErrors(t *testing.T) {
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"payload must not be empty","code":"bad_request"}`))
	})
	err := b.post("/jobs", map[string]any{"type": "shell"}, nil)
	if err == nil {
		t.Fatal("want an error for a 400 response, got nil")
	}
	if !strings.Contains(err.Error(), "payload must not be empty") {
		t.Errorf("error = %q; want it to carry the server's message", err)
	}
}
