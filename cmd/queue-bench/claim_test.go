package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestParseLevels(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  []int
		valid bool
	}{
		{"plain list", "1,4,16,64", []int{1, 4, 16, 64}, true},
		{"tolerates spaces and a trailing comma", " 2 , 8, ", []int{2, 8}, true},
		{"single level", "32", []int{32}, true},
		{"rejects zero", "0", nil, false},
		{"rejects negative", "4,-1", nil, false},
		{"rejects non-numeric", "4,lots", nil, false},
		{"rejects empty", "", nil, false},
		{"rejects commas only", ",,", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLevels(tt.in)
			if tt.valid != (err == nil) {
				t.Fatalf("parseLevels(%q) error = %v; want valid=%v", tt.in, err, tt.valid)
			}
			if !tt.valid {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseLevels(%q) = %v; want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("parseLevels(%q) = %v; want %v", tt.in, got, tt.want)
				}
			}
		})
	}
}

// A zero level would start no goroutines, so the level would "drain" instantly
// and report an infinite rate rather than failing.
func TestParseLevelsRejectsZeroBeforeItBecomesAnInfiniteRate(t *testing.T) {
	if _, err := parseLevels("0"); err == nil {
		t.Fatal("want an error for a zero claimer count, got nil")
	}
}

func TestHasCode(t *testing.T) {
	empty := &httpError{Status: 404, Code: "no_jobs_available", Msg: "no jobs"}
	if !hasCode(empty, "no_jobs_available") {
		t.Error("hasCode did not match the error's own code")
	}
	if hasCode(empty, "not_found") {
		t.Error("hasCode matched a different code")
	}
	// A 404 from an unrelated cause must not read as an empty queue.
	notFound := &httpError{Status: 404, Code: "not_found", Msg: "unknown worker"}
	if hasCode(notFound, "no_jobs_available") {
		t.Error("a 404 not_found was treated as an empty queue")
	}
	if hasCode(fmt.Errorf("connection refused"), "no_jobs_available") {
		t.Error("a plain error was treated as an empty queue")
	}
}

// claimOne must distinguish an empty queue from a failure. Reporting an empty
// queue as an error would abort the run; reporting a real failure as "empty"
// would silently end the level early and publish a rate for jobs never claimed.
func TestClaimOneSeparatesEmptyQueueFromFailure(t *testing.T) {
	t.Run("empty queue is not an error", func(t *testing.T) {
		b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no jobs available","code":"no_jobs_available"}`))
		})
		id, got, err := b.claimOne(map[string]any{"worker_id": "w"})
		if err != nil || got || id != 0 {
			t.Fatalf("claimOne = (%d, %v, %v); want (0, false, nil)", id, got, err)
		}
	})

	t.Run("a server failure is an error", func(t *testing.T) {
		b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"pool exhausted","code":"internal"}`))
		})
		if _, got, err := b.claimOne(map[string]any{"worker_id": "w"}); err == nil || got {
			t.Fatalf("claimOne got=%v err=%v; want an error", got, err)
		}
	})

	t.Run("a claim returns the job id", func(t *testing.T) {
		b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":41,"status":"RUNNING"}`))
		})
		id, got, err := b.claimOne(map[string]any{"worker_id": "w"})
		if err != nil || !got || id != 41 {
			t.Fatalf("claimOne = (%d, %v, %v); want (41, true, nil)", id, got, err)
		}
	})
}

// SKIP LOCKED reports "no rows" when every remaining candidate is locked, not
// only when the table is empty, so a claimer that quits on the first empty
// answer abandons jobs that are still PENDING. This is the regression test for
// the second look: a queue that says empty, then yields a job, must not end the
// level at the first empty response.
func TestClaimUntilEmptyConfirmsBeforeGivingUp(t *testing.T) {
	var n atomic.Int32
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			_, _ = w.Write([]byte(`{"id":1}`))
		case 2: // a spurious empty: rows remain, but they are all locked
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"no_jobs_available"}`))
		case 3:
			_, _ = w.Write([]byte(`{"id":2}`))
		default: // genuinely drained: two empties in a row
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"no_jobs_available"}`))
		}
	})

	claims, err := b.claimUntilEmpty("w1")
	if err != nil {
		t.Fatalf("claimUntilEmpty: %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("claimed %d job(s); want 2 -- the claimer quit at the first empty response", len(claims))
	}
	for i, want := range []int64{1, 2} {
		if claims[i].jobID != want || claims[i].worker != "w1" {
			t.Errorf("claims[%d] = %+v; want job %d for w1", i, claims[i], want)
		}
	}
}

// An error mid-drain must propagate rather than being counted as a clean end,
// which would publish a rate over a backlog that was never claimed.
func TestClaimUntilEmptyPropagatesFailures(t *testing.T) {
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream gone","code":"bad_gateway"}`))
	})
	if _, err := b.claimUntilEmpty("w1"); err == nil {
		t.Fatal("want an error when the server fails, got nil")
	}
}

// Our own claimers heartbeat as a side effect of claiming, so counting them
// would make a second run inside the heartbeat timeout refuse to start.
func TestForeignAliveWorkersIgnoresOurOwnClaimers(t *testing.T) {
	b := newTestBench(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"workers":[
			{"id":"bench-claimer-64-3","alive":true},
			{"id":"bench-claimer-1-0","alive":false},
			{"id":"worker-abc","alive":true},
			{"id":"worker-dead","alive":false}
		]}`))
	})
	n, err := b.foreignAliveWorkers()
	if err != nil {
		t.Fatalf("foreignAliveWorkers: %v", err)
	}
	if n != 1 {
		t.Fatalf("foreignAliveWorkers = %d; want 1 (only the live non-bench worker)", n)
	}
}

// The correctness verdict is the point of the mode; a duplicate must never be
// rendered as the reassuring "0 ... claimed twice" line.
func TestClaimVerdictReportsDuplicatesAsFailure(t *testing.T) {
	if got := claimVerdict(100, 0); !strings.HasPrefix(got, "0 of 100") {
		t.Errorf("verdict with no duplicates = %q", got)
	}
	got := claimVerdict(99, 1)
	if !strings.Contains(got, "FAILED") {
		t.Errorf("verdict with a duplicate = %q; want it to say FAILED", got)
	}
}
