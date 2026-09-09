package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// Catches: stdout not being captured, or a successful command not recording
// exit code 0 (leaving it nil, which the API would render as "unknown").
func TestExecuteShellSuccess(t *testing.T) {
	res, err := ExecuteShell(context.Background(), "echo hello", 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(res.Stdout) != "hello" {
		t.Errorf("stdout = %q, want %q", res.Stdout, "hello")
	}
	if res.ExitCode == nil || *res.ExitCode != 0 {
		t.Errorf("exit code = %v, want 0", res.ExitCode)
	}
	if res.Duration <= 0 {
		t.Error("duration was not measured")
	}
}

// Catches: a non-zero exit being reported as success, and stderr being dropped.
func TestExecuteShellFailureCapturesExitCodeAndStderr(t *testing.T) {
	res, err := ExecuteShell(context.Background(), "echo oops >&2; exit 7", 5*time.Second)
	if err == nil {
		t.Fatal("expected an error for a non-zero exit")
	}
	if res.ExitCode == nil || *res.ExitCode != 7 {
		t.Errorf("exit code = %v, want 7", res.ExitCode)
	}
	if !strings.Contains(res.Stderr, "oops") {
		t.Errorf("stderr = %q, want it to contain %q", res.Stderr, "oops")
	}
}

// Catches: a hung job blocking the worker forever, and a timeout being reported
// as an ordinary command failure so the operator cannot tell them apart.
func TestExecuteShellTimeout(t *testing.T) {
	start := time.Now()
	_, err := ExecuteShell(context.Background(), "sleep 30", 300*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("error = %v, want it to wrap ErrTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %v; the process was not actually killed", elapsed)
	}
}

// Catches: cancellation of the parent context (worker shutdown, or a lost lease)
// failing to stop a running command.
func TestExecuteShellHonorsParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := ExecuteShell(ctx, "sleep 30", time.Minute); err == nil {
		t.Fatal("expected an error when the parent context is canceled")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %v; cancellation did not kill the command", elapsed)
	}
}

// Catches: an unbounded buffer letting a runaway command exhaust worker memory.
func TestExecuteShellBoundsOutput(t *testing.T) {
	// Emits far more than MaxOutputBytes.
	res, err := ExecuteShell(context.Background(),
		"for i in $(seq 1 5000); do echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; done", 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Stdout) > MaxOutputBytes+128 {
		t.Errorf("stdout length = %d, want it bounded near %d", len(res.Stdout), MaxOutputBytes)
	}
	if !strings.Contains(res.Stdout, "truncated") {
		t.Error("bounded output does not report that it was truncated")
	}
}

// Catches: a command that cannot start being misreported with a bogus exit code
// instead of a "failed to start" error.
func TestExecuteShellUnknownCommand(t *testing.T) {
	res, err := ExecuteShell(context.Background(), "definitely-not-a-real-command-xyz", 5*time.Second)
	if err == nil {
		t.Fatal("expected an error")
	}
	// `sh -c` itself starts fine and exits 127, so this asserts the code is
	// surfaced rather than swallowed.
	if res.ExitCode == nil || *res.ExitCode == 0 {
		t.Errorf("exit code = %v, want a non-zero code", res.ExitCode)
	}
}
