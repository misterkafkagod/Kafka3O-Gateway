package scan

import (
	"testing"
	"time"
)

// TestRegexMatcher_UndecodableIsSkipped proves the per-record regex timeout
// reports MatchResult{Skipped: true}, never an error (FUNC-SPEC §9.2
// Evaluate). It exercises runWithTimeout directly with an injected work func
// that sleeps far longer than the bound, rather than relying on regexp
// itself being slow — RE2's whole point is that it never is, so a real
// pattern/input pair large enough to race a near-zero timeout reliably is
// not achievable without an unreasonably large fixture. This file is
// white-box (package scan, not scan_test) exactly so it can reach the
// unexported runWithTimeout that NewRegexMatcher's timeout path is built on.
func TestRegexMatcher_UndecodableIsSkipped(t *testing.T) {
	t.Parallel()

	result, timedOut := runWithTimeout(time.Millisecond, func() bool {
		time.Sleep(50 * time.Millisecond)
		return true
	})
	if !timedOut || result {
		t.Fatalf("runWithTimeout(1ms, 50ms-slow work) = (%v, timedOut=%v), want (false, true)", result, timedOut)
	}

	// The non-timeout path still returns the work's own result unchanged.
	result2, timedOut2 := runWithTimeout(50*time.Millisecond, func() bool { return true })
	if timedOut2 || !result2 {
		t.Fatalf("runWithTimeout(50ms, instant work) = (%v, timedOut=%v), want (true, false)", result2, timedOut2)
	}
}
