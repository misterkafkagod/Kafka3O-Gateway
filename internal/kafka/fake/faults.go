package fake

import (
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// FailNext makes the next call to method return a *kafka.Error{Kind: kind};
// it then clears itself, so the call after that succeeds normally (TECH-SPEC
// §4.3 Fault injection row).
func (f *Fake) FailNext(method string, kind kafka.Kind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext[method] = kind
}

// FailAlways makes every future call to method return a *kafka.Error{Kind:
// kind} until the test overwrites or the Fake is discarded.
func (f *Fake) FailAlways(method string, kind kafka.Kind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAlways[method] = kind
}

// Latency delays every future call to method by d before it runs, honouring
// ctx: if ctx expires first, the call returns *kafka.Error{Kind: KindTimeout}
// instead of waiting out the full delay.
func (f *Fake) Latency(method string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latency[method] = d
}

// Unreachable, when true, makes every call return
// *kafka.Error{Kind: KindUnavailable} without touching the model.
func (f *Fake) Unreachable(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreachable = v
}

// Calls returns every recorded call, in order.
func (f *Fake) Calls() []CallRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]CallRecord, len(f.calls))
	copy(out, f.calls)
	return out
}

// MutatingCalls returns the subset of Calls that mutated the model.
func (f *Fake) MutatingCalls() []CallRecord {
	var out []CallRecord
	for _, c := range f.Calls() {
		if c.Mutating {
			out = append(out, c)
		}
	}
	return out
}

// AssertCalled fails t unless method appears at least once in Calls.
func (f *Fake) AssertCalled(t *testing.T, method string) {
	t.Helper()
	for _, c := range f.Calls() {
		if c.Method == method {
			return
		}
	}
	t.Errorf("fake: %s was never called", method)
}

// AssertNoCommits fails t if any offset-committing call was made (FUNC-SPEC
// O4: reads never mutate cluster state).
func (f *Fake) AssertNoCommits(t *testing.T) {
	t.Helper()
	for _, c := range f.Calls() {
		if c.Method == "CommitGroupOffsets" {
			t.Errorf("fake: unexpected commit via %s", c.Method)
		}
	}
}

// AssertNoGroupJoin fails t if any consumer-group membership call was made
// (FUNC-SPEC O4, §9.1: manual partition assignment only, never a group).
func (f *Fake) AssertNoGroupJoin(t *testing.T) {
	t.Helper()
	for _, c := range f.Calls() {
		if c.Method == "JoinGroup" || c.Method == "SyncGroup" {
			t.Errorf("fake: unexpected group membership call %s", c.Method)
		}
	}
}
