// Package worker contains the worker process: an HTTP client for the queue API,
// a shell executor, and the claim-execute-report loop that joins them.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

// Client is a thin typed wrapper over the queue's REST API.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewClient returns a client with a bounded timeout.
//
// The default http.Client has no timeout at all, so a server that accepts a
// connection and then stops responding would hang a worker forever -- and a
// hung worker still holds a lease, blocking the job it will never finish.
func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{Timeout: 15 * time.Second},
	}
}

// ErrNoJobs means the queue had nothing available; it is the normal idle case.
var ErrNoJobs = errors.New("no jobs available")

// ErrUnknownWorker means the server has no record of us, so we must re-register.
var ErrUnknownWorker = errors.New("unknown worker")

// APIError carries a non-2xx response.
type APIError struct {
	Status int
	Code   string
	Msg    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("queue server returned %d (%s): %s", e.Status, e.Code, e.Msg)
}

// do performs one request and decodes the response into out.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		// Read the structured error, but tolerate a body that is not JSON (a
		// proxy's HTML error page, for instance) rather than masking the status.
		var er struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		_ = json.Unmarshal(raw, &er)
		if er.Code == "no_jobs_available" {
			return ErrNoJobs
		}
		if er.Code == "not_found" && er.Error == "unknown worker: register first" {
			return ErrUnknownWorker
		}
		msg := er.Error
		if msg == "" {
			msg = string(bytes.TrimSpace(raw))
		}
		return &APIError{Status: resp.StatusCode, Code: er.Code, Msg: msg}
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// Register announces this worker to the server.
func (c *Client) Register(ctx context.Context, workerID, hostname string) error {
	return c.do(ctx, http.MethodPost, "/workers/register",
		map[string]string{"worker_id": workerID, "hostname": hostname}, nil)
}

// Heartbeat reports liveness.
func (c *Client) Heartbeat(ctx context.Context, workerID string) error {
	return c.do(ctx, http.MethodPost, "/workers/"+workerID+"/heartbeat", nil, nil)
}

// Claim asks for one job. It returns ErrNoJobs when the queue is empty.
func (c *Client) Claim(ctx context.Context, workerID string, lease time.Duration) (*jobs.Job, error) {
	var job jobs.Job
	err := c.do(ctx, http.MethodPost, "/jobs/claim", map[string]any{
		"worker_id": workerID, "lease_seconds": lease.Seconds(),
	}, &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// RenewLease extends the lease on a job still being executed.
func (c *Client) RenewLease(ctx context.Context, id int64, workerID string, lease time.Duration) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/jobs/%d/lease", id),
		map[string]any{"worker_id": workerID, "lease_seconds": lease.Seconds()}, nil)
}

// Complete reports success.
func (c *Client) Complete(ctx context.Context, id int64, workerID string, r ExecResult) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/jobs/%d/complete", id), map[string]any{
		"worker_id": workerID, "stdout": r.Stdout, "stderr": r.Stderr,
		"exit_code": r.ExitCode, "duration_ms": r.DurationMS(),
	}, nil)
}

// Fail reports a failed attempt; the server decides whether to retry.
func (c *Client) Fail(ctx context.Context, id int64, workerID string, r ExecResult, cause string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/jobs/%d/fail", id), map[string]any{
		"worker_id": workerID, "stdout": r.Stdout, "stderr": r.Stderr,
		"exit_code": r.ExitCode, "duration_ms": r.DurationMS(), "error": cause,
	}, nil)
}
