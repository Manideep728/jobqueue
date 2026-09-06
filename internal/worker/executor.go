package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// ExecResult is everything observed about one execution attempt.
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode *int // nil when the process never started or was killed by a signal
	Duration time.Duration
}

// DurationMS renders the duration for the API.
func (r ExecResult) DurationMS() int64 { return r.Duration.Milliseconds() }

// MaxOutputBytes caps how much of a command's output is kept in memory.
//
// Without a cap, a job like `yes` would grow the buffer until the worker is
// killed by the OOM killer -- and a worker killed that way takes its lease with
// it, so the job gets retried and kills the next worker too.
const MaxOutputBytes = 32 * 1024

// boundedBuffer is an io.Writer that stores at most Limit bytes while counting
// everything written, so truncation can be reported honestly. It never returns
// an error, because a short write would make exec kill the command with a
// confusing "broken pipe" instead of letting it finish.
//
// The mutex is required: exec writes stdout and stderr from separate goroutines,
// and although each stream has its own buffer here, String() is called from the
// worker goroutine while those may still be finishing.
type boundedBuffer struct {
	mu    sync.Mutex
	buf   []byte
	total int // bytes written, including those not stored
	Limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total += len(p)
	if room := b.Limit - len(b.buf); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		b.buf = append(b.buf, p[:room]...)
	}
	// Always report a full write: the bytes beyond the limit are intentionally
	// discarded, not a failure the command should hear about.
	return len(p), nil
}

// String returns the captured output, noting truncation when it happened.
func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.total > len(b.buf) {
		return string(b.buf) + fmt.Sprintf("\n...[truncated, %d bytes total]", b.total)
	}
	return string(b.buf)
}

// ErrTimeout means the command exceeded the per-job execution timeout.
var ErrTimeout = errors.New("job execution timed out")

// ExecuteShell runs payload through `sh -c` and captures the outcome.
//
// SECURITY: this is arbitrary code execution by design -- that is what a "shell"
// job type means. There is no sandbox and no attempt at one; the protection is
// operational (bind to localhost, optional bearer token, run as an unprivileged
// user, ideally inside a container). Never expose this queue to input you do not
// control.
func ExecuteShell(ctx context.Context, payload string, timeout time.Duration) (ExecResult, error) {
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(ctx, "sh", "-c", payload)

	// Setpgid puts the command in its own process group. `sh -c "sleep 100"` may
	// exec a child; killing only the shell would leave that child running as an
	// orphan holding the pipes open. Signalling the whole group kills the tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid means "the process group with this id".
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	// If the process ignores the kill, stop waiting on its pipes anyway rather
	// than blocking this worker forever.
	cmd.WaitDelay = 5 * time.Second

	stdout := &boundedBuffer{Limit: MaxOutputBytes}
	stderr := &boundedBuffer{Limit: MaxOutputBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	runErr := cmd.Run()
	res := ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: time.Since(start),
	}

	// A timeout is reported as a timeout, not as "exit status -1": the two have
	// completely different causes and the operator needs to tell them apart.
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return res, fmt.Errorf("%w after %s", ErrTimeout, timeout)
	}
	if runErr == nil {
		code := 0
		res.ExitCode = &code
		return res, nil
	}

	// A non-zero exit is an ExitError and carries a real exit code. Anything else
	// (command not found, fork failure) means the process never ran.
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		code := exitErr.ExitCode()
		res.ExitCode = &code
		return res, fmt.Errorf("command exited with status %d", code)
	}
	return res, fmt.Errorf("failed to start command: %w", runErr)
}
