// Command queue is the command line client for the queue server.
//
//	queue submit --type shell --payload "echo hello"
//	queue get 42
//	queue list --status pending
//	queue cancel 42
//	queue workers
//	queue stats
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/manideep7286/queue/internal/jobs"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

const usage = `queue - client for the QUEUE job queue

Usage:
  queue submit --type shell --payload "echo hello" [--max-attempts 3] [--wait]
  queue get <id>
  queue list [--status pending] [--limit 20]
  queue cancel <id>
  queue workers
  queue stats

Global flags (may appear after the subcommand):
  --server URL   queue server (default $QUEUE_SERVER or http://localhost:8080)
  --json         print the raw JSON response

Environment:
  QUEUE_SERVER      default server URL
  QUEUE_AUTH_TOKEN  bearer token, when the server requires one
`

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return errors.New("no command given")
	}
	switch args[0] {
	case "submit":
		return cmdSubmit(args[1:], out)
	case "get":
		return cmdGet(args[1:], out)
	case "list":
		return cmdList(args[1:], out)
	case "cancel":
		return cmdCancel(args[1:], out)
	case "workers":
		return cmdWorkers(args[1:], out)
	case "stats":
		return cmdStats(args[1:], out)
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return nil
	default:
		fmt.Fprint(out, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

func cmdSubmit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	typ := fs.String("type", jobs.TypeShell, "job type")
	payload := fs.String("payload", "", "command to run")
	maxAttempts := fs.Int("max-attempts", 3, "how many times to try before failing")
	wait := fs.Bool("wait", false, "block until the job reaches a terminal state")
	waitFor := fs.Duration("wait-timeout", 2*time.Minute, "how long --wait waits")
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*payload) == "" {
		return errors.New("--payload is required")
	}

	var job jobs.Job
	err := request(*server, http.MethodPost, "/jobs", map[string]any{
		"type": *typ, "payload": *payload, "max_attempts": *maxAttempts,
	}, &job)
	if err != nil {
		return err
	}

	if *wait {
		final, err := waitForTerminal(*server, job.ID, *waitFor)
		if err != nil {
			return err
		}
		job = *final
		if *asJSON {
			return printJSON(out, job)
		}
		printJob(out, &job)
		// A non-zero exit lets `queue submit --wait ... && next-thing` work in a
		// shell script the way any other command would.
		if job.Status != jobs.StatusCompleted {
			return fmt.Errorf("job %d finished with status %s", job.ID, job.Status)
		}
		return nil
	}

	if *asJSON {
		return printJSON(out, job)
	}
	fmt.Fprintf(out, "Job submitted successfully.\nID: %d\nStatus: %s\n", job.ID, job.Status)
	return nil
}

func cmdGet(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := positionalID(fs)
	if err != nil {
		return err
	}
	var job jobs.Job
	if err := request(*server, http.MethodGet, "/jobs/"+strconv.FormatInt(id, 10), nil, &job); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, job)
	}
	printJob(out, &job)
	return nil
}

func cmdList(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	status := fs.String("status", "", "filter: pending, running, completed, failed, canceled")
	limit := fs.Int("limit", 20, "maximum rows")
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := fmt.Sprintf("/jobs?limit=%d", *limit)
	if *status != "" {
		path += "&status=" + strings.ToUpper(*status)
	}
	var resp struct {
		Jobs  []*jobs.Job `json:"jobs"`
		Count int         `json:"count"`
	}
	if err := request(*server, http.MethodGet, path, nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, resp)
	}
	if resp.Count == 0 {
		fmt.Fprintln(out, "No jobs.")
		return nil
	}
	// Fixed-width columns keep the output scannable without a table library.
	fmt.Fprintf(out, "%-6s %-10s %-9s %-22s %s\n", "ID", "STATUS", "ATTEMPTS", "WORKER", "PAYLOAD")
	for _, j := range resp.Jobs {
		fmt.Fprintf(out, "%-6d %-10s %-9s %-22s %s\n",
			j.ID, j.Status, fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts),
			strVal(j.WorkerID, "-"), truncate(j.Payload, 40))
	}
	return nil
}

func cmdCancel(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := positionalID(fs)
	if err != nil {
		return err
	}
	var job jobs.Job
	if err := request(*server, http.MethodPost,
		fmt.Sprintf("/jobs/%d/cancel", id), map[string]any{}, &job); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, job)
	}
	fmt.Fprintf(out, "Job %d canceled.\n", job.ID)
	return nil
}

func cmdWorkers(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("workers", flag.ContinueOnError)
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var resp struct {
		Workers []*jobs.Worker `json:"workers"`
		Count   int            `json:"count"`
	}
	if err := request(*server, http.MethodGet, "/workers", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, resp)
	}
	if resp.Count == 0 {
		fmt.Fprintln(out, "No workers registered.")
		return nil
	}
	fmt.Fprintf(out, "%-26s %-8s %-10s %-11s %s\n", "ID", "ALIVE", "PROCESSED", "CURRENT JOB", "LAST HEARTBEAT")
	for _, w := range resp.Workers {
		current := "-"
		if w.CurrentJobID != nil {
			current = strconv.FormatInt(*w.CurrentJobID, 10)
		}
		fmt.Fprintf(out, "%-26s %-8t %-10d %-11s %s ago\n", w.ID, w.Alive, w.JobsProcessed,
			current, time.Since(w.LastHeartbeat).Truncate(time.Second))
	}
	return nil
}

func cmdStats(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	server, asJSON := commonFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var resp struct {
		Jobs         map[string]int64 `json:"jobs"`
		WorkersTotal int              `json:"workers_total"`
		WorkersAlive int              `json:"workers_alive"`
	}
	if err := request(*server, http.MethodGet, "/stats", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, resp)
	}
	fmt.Fprintln(out, "Jobs:")
	// Iterate a fixed list rather than the map: Go randomizes map order, which
	// would reshuffle the output on every run.
	for _, s := range []jobs.Status{jobs.StatusPending, jobs.StatusRunning,
		jobs.StatusCompleted, jobs.StatusFailed, jobs.StatusCanceled} {
		fmt.Fprintf(out, "  %-10s %d\n", string(s)+":", resp.Jobs[string(s)])
	}
	fmt.Fprintf(out, "Workers: %d alive / %d total\n", resp.WorkersAlive, resp.WorkersTotal)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func commonFlags(fs *flag.FlagSet) (server *string, asJSON *bool) {
	def := os.Getenv("QUEUE_SERVER")
	if def == "" {
		def = "http://localhost:8080"
	}
	server = fs.String("server", def, "queue server base URL")
	asJSON = fs.Bool("json", false, "print raw JSON")
	return server, asJSON
}

// positionalID reads the <id> argument left over after flag parsing.
func positionalID(fs *flag.FlagSet) (int64, error) {
	if fs.NArg() < 1 {
		return 0, errors.New("missing job id")
	}
	id, err := strconv.ParseInt(fs.Arg(0), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid job id %q: must be a positive integer", fs.Arg(0))
	}
	return id, nil
}

// request performs one API call and decodes the result.
func request(server, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(server, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("QUEUE_AUTH_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// The overwhelmingly common cause is "the server is not running", so say
		// so instead of showing a raw dial error.
		return fmt.Errorf("cannot reach queue server at %s: %w", server, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var er struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		_ = json.Unmarshal(raw, &er)
		if er.Error == "" {
			er.Error = strings.TrimSpace(string(raw))
		}
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, er.Error)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// waitForTerminal polls a job until it stops moving.
//
// Polling rather than streaming keeps the server stateless and the client
// trivial; at CLI timescales the extra requests cost nothing.
func waitForTerminal(server string, id int64, timeout time.Duration) (*jobs.Job, error) {
	deadline := time.Now().Add(timeout)
	for {
		var job jobs.Job
		if err := request(server, http.MethodGet, fmt.Sprintf("/jobs/%d", id), nil, &job); err != nil {
			return nil, err
		}
		switch job.Status {
		case jobs.StatusCompleted, jobs.StatusFailed, jobs.StatusCanceled:
			return &job, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("job %d still %s after %s", id, job.Status, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func printJob(out io.Writer, j *jobs.Job) {
	fmt.Fprintf(out, "ID:        %d\n", j.ID)
	fmt.Fprintf(out, "Status:    %s\n", j.Status)
	fmt.Fprintf(out, "Type:      %s\n", j.Type)
	fmt.Fprintf(out, "Payload:   %s\n", j.Payload)
	fmt.Fprintf(out, "Attempts:  %d/%d\n", j.Attempts, j.MaxAttempts)
	fmt.Fprintf(out, "Worker:    %s\n", strVal(j.WorkerID, "-"))
	fmt.Fprintf(out, "Created:   %s\n", j.CreatedAt.Local().Format(time.RFC3339))
	if j.DurationMS != nil {
		fmt.Fprintf(out, "Duration:  %dms\n", *j.DurationMS)
	}
	if j.ResultExitCode != nil {
		fmt.Fprintf(out, "Exit code: %d\n", *j.ResultExitCode)
	}
	if j.LeaseUntil != nil {
		fmt.Fprintf(out, "Lease:     until %s\n", j.LeaseUntil.Local().Format(time.RFC3339))
	}
	if j.Status == jobs.StatusPending && j.Attempts > 0 {
		fmt.Fprintf(out, "Retry at:  %s\n", j.AvailableAt.Local().Format(time.RFC3339))
	}
	if s := strVal(j.LastError, ""); s != "" {
		fmt.Fprintf(out, "Error:     %s\n", s)
	}
	if s := strVal(j.ResultStdout, ""); s != "" {
		fmt.Fprintf(out, "--- stdout ---\n%s", ensureNewline(s))
	}
	if s := strVal(j.ResultStderr, ""); s != "" {
		fmt.Fprintf(out, "--- stderr ---\n%s", ensureNewline(s))
	}
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func strVal(p *string, def string) string {
	if p == nil || *p == "" {
		return def
	}
	return *p
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func ensureNewline(s string) string {
	if strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}
