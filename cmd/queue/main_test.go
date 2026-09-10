package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeServer stands in for queue-server so the CLI can be tested without a
// database. It asserts on what the CLI sends and controls what comes back.
func fakeServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// Catches: the CLI sending the wrong field names or dropping flags, which would
// silently create jobs with the wrong type or attempt budget.
func TestSubmitSendsCorrectRequest(t *testing.T) {
	var got map[string]any
	srv := fakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/jobs" {
			t.Errorf("got %s %s, want POST /jobs", r.Method, r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":42,"status":"PENDING"}`))
	})

	var out bytes.Buffer
	err := run([]string{"submit", "--server", srv.URL, "--type", "shell",
		"--payload", "echo hi", "--max-attempts", "7"}, &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["type"] != "shell" || got["payload"] != "echo hi" || got["max_attempts"] != float64(7) {
		t.Errorf("request body = %v, want type/payload/max_attempts to match the flags", got)
	}
	if !strings.Contains(out.String(), "ID: 42") {
		t.Errorf("output = %q, want it to report the new job id", out.String())
	}
}

// Catches: a missing payload silently submitting an empty job.
func TestSubmitRequiresPayload(t *testing.T) {
	var out bytes.Buffer
	err := run([]string{"submit", "--server", "http://localhost:1", "--type", "shell"}, &out)
	if err == nil {
		t.Fatal("expected an error when --payload is missing")
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Errorf("error = %q, want it to name the missing flag", err)
	}
}

// Catches: a bad id being sent to the server as a malformed URL instead of being
// rejected locally with a clear message.
func TestGetRejectsInvalidID(t *testing.T) {
	var out bytes.Buffer
	for _, arg := range []string{"abc", "-1", "0", ""} {
		args := []string{"get", "--server", "http://localhost:1"}
		if arg != "" {
			args = append(args, arg)
		}
		if err := run(args, &out); err == nil {
			t.Errorf("get %q: expected an error", arg)
		}
	}
}

// Catches: a server error being reported as success, which would make
// `queue submit ... && deploy` run after a failure.
func TestServerErrorIsSurfaced(t *testing.T) {
	srv := fakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"job not found","code":"not_found"}`))
	})
	var out bytes.Buffer
	err := run([]string{"get", "--server", srv.URL, "999"}, &out)
	if err == nil {
		t.Fatal("expected an error for a 404 response")
	}
	if !strings.Contains(err.Error(), "job not found") {
		t.Errorf("error = %q, want it to include the server's message", err)
	}
}

// Catches: an unreachable server producing a raw dial error rather than a
// message a user can act on.
func TestUnreachableServerMessage(t *testing.T) {
	var out bytes.Buffer
	// Port 1 is reserved and nothing listens there.
	err := run([]string{"list", "--server", "http://127.0.0.1:1"}, &out)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "cannot reach queue server") {
		t.Errorf("error = %q, want a message naming the unreachable server", err)
	}
}

// Catches: the list view crashing on a job with no worker assigned (a nil
// pointer), which is the state of every PENDING job.
func TestListRendersJobsWithoutWorker(t *testing.T) {
	srv := fakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "status=PENDING") {
			t.Errorf("query = %q, want the status filter to be forwarded uppercased", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"count":1,"jobs":[{"id":1,"status":"PENDING","attempts":0,
			"max_attempts":3,"payload":"echo hi","worker_id":null}]}`))
	})
	var out bytes.Buffer
	if err := run([]string{"list", "--server", srv.URL, "--status", "pending"}, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "PENDING") || !strings.Contains(out.String(), "echo hi") {
		t.Errorf("output = %q, want the job listed", out.String())
	}
}

// Catches: an empty result printing a bare header that looks like a bug.
func TestListEmpty(t *testing.T) {
	srv := fakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"count":0,"jobs":[]}`))
	})
	var out bytes.Buffer
	if err := run([]string{"list", "--server", srv.URL}, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "No jobs") {
		t.Errorf("output = %q, want a clear empty message", out.String())
	}
}

// Catches: an unknown subcommand being silently ignored.
func TestUnknownCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"frobnicate"}, &out); err == nil {
		t.Fatal("expected an error for an unknown command")
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Error("an unknown command should print usage")
	}
}

// Catches: --json emitting the human format, breaking `queue list --json | jq`.
func TestJSONOutput(t *testing.T) {
	srv := fakeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"count":0,"jobs":[]}`))
	})
	var out bytes.Buffer
	if err := run([]string{"list", "--server", srv.URL, "--json"}, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out.Bytes(), &parsed); err != nil {
		t.Fatalf("--json output is not valid JSON: %v (%q)", err, out.String())
	}
}
