package jobs

import (
	"strings"
	"testing"
	"time"
)

// Catches: an off-by-one in the exponent (attempt 1 waiting 2s instead of 1s),
// and a missing cap letting attempt 20 schedule a retry days into the future.
func TestBackoffDelay(t *testing.T) {
	c := BackoffConfig{Base: time.Second, Max: 5 * time.Minute}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{9, 256 * time.Second},
		{10, 5 * time.Minute}, // 512s would exceed the cap
		{50, 5 * time.Minute}, // far past the cap
	}
	for _, tc := range cases {
		if got := c.Delay(tc.attempt); got != tc.want {
			t.Errorf("Delay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// Catches: an attempt count so large that 2^attempt overflows int64 and wraps to
// a negative duration, which would make a job instantly retryable forever.
func TestBackoffNeverNegative(t *testing.T) {
	c := BackoffConfig{Base: time.Second, Max: time.Minute}
	for _, attempt := range []int{-5, 0, 1, 62, 63, 64, 1000, 1 << 30} {
		if got := c.Delay(attempt); got <= 0 || got > time.Minute {
			t.Errorf("Delay(%d) = %v, want a positive duration <= 1m", attempt, got)
		}
	}
}

// Catches: a zero-value config dividing by zero or returning a 0s delay, which
// would turn a failing job into a hot retry loop.
func TestBackoffZeroConfigHasSaneDefaults(t *testing.T) {
	var c BackoffConfig
	if got := c.Delay(1); got != time.Second {
		t.Errorf("zero-config Delay(1) = %v, want 1s", got)
	}
}

// Catches: validation that lets an empty payload, an unknown job type, or a
// max_attempts of 0 (a job that can never run) reach the database.
func TestValidateNew(t *testing.T) {
	cases := []struct {
		name        string
		typ         string
		payload     string
		maxAttempts int
		wantErr     string
	}{
		{"valid", "shell", "echo hi", 3, ""},
		{"empty type", "", "echo hi", 3, "type is required"},
		{"unknown type", "python", "print(1)", 3, "unsupported job type"},
		{"empty payload", "shell", "", 3, "payload is required"},
		{"whitespace payload", "shell", "   \n\t ", 3, "payload is required"},
		{"zero attempts", "shell", "echo hi", 0, "max_attempts must be >= 1"},
		{"negative attempts", "shell", "echo hi", -1, "max_attempts must be >= 1"},
		{"absurd attempts", "shell", "echo hi", 10000, "max_attempts must be <= 100"},
		{"oversized payload", "shell", strings.Repeat("x", MaxPayloadBytes+1), 3, "exceeds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNew(tc.typ, tc.payload, tc.maxAttempts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// Catches: a status typo in the API layer being accepted and then rejected by
// the database CHECK constraint as an opaque 500.
func TestValidStatus(t *testing.T) {
	for _, s := range []Status{StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCanceled} {
		if !ValidStatus(s) {
			t.Errorf("ValidStatus(%q) = false, want true", s)
		}
	}
	for _, s := range []Status{"", "pending", "DONE", "RETRY"} {
		if ValidStatus(s) {
			t.Errorf("ValidStatus(%q) = true, want false", s)
		}
	}
}

// Catches: unbounded output being written to the database, and truncation that
// silently hides the fact that output was cut.
func TestTruncateOutput(t *testing.T) {
	short := "hello"
	if got := TruncateOutput(short); got != short {
		t.Errorf("short output was modified: %q", got)
	}
	long := strings.Repeat("a", MaxResultBytes+500)
	got := TruncateOutput(long)
	if len(got) > MaxResultBytes+64 {
		t.Errorf("truncated length = %d, want about %d", len(got), MaxResultBytes)
	}
	if !strings.HasSuffix(got, "[truncated]") {
		t.Error("truncated output does not say it was truncated")
	}
}
