// Package fake is the in-memory test double for internal/kafka (TECH-SPEC
// §4.3): it implements kafka.Admin, kafka.Consumer, and kafka.Producer so the
// automated suite never connects to a real cluster (FUNC-SPEC V1). It is a
// test double only — imported exclusively from _test.go files (TECH-SPEC P3,
// enforced by depguard); the production import graph never reaches it.
//
// Compaction is not simulated: every record ever seeded or produced stays in
// its partition's log forever, so tests that need compacted-topic behaviour
// (deletion at a key) must assert on the tombstone record itself, not on the
// key disappearing.
package fake

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// CallRecord is one recorded port call (TECH-SPEC §4.3 Recording row).
type CallRecord struct {
	Method   string
	Mutating bool
	Err      error
}

// Fake is the test double. All state is guarded by mu; every exported method
// is safe for concurrent use.
type Fake struct {
	mu sync.Mutex

	now   func() time.Time
	model model

	calls       []CallRecord
	failNext    map[string]kafka.Kind
	failAlways  map[string]kafka.Kind
	latency     map[string]time.Duration
	unreachable bool

	// consumerSession is the fake's single active manual-assignment session
	// (TECH-SPEC §4.3): one Assign replaces any previous session, mirroring
	// a dedicated per-scan client (no group, no commits — FUNC-SPEC O4).
	consumerSession *consumerSession
}

// Option configures a Fake at construction.
type Option func(*Fake)

// WithClock injects the clock used for produced and seeded record
// timestamps — the same signature internal/scan.Runner accepts, so a test
// can share one fixed clock across the fake and the scan loop (TECH-SPEC §4.3
// Time row).
func WithClock(now func() time.Time) Option {
	return func(f *Fake) { f.now = now }
}

// New builds an empty Fake. Every fault-injection and seeding method may be
// called before or between port calls; construction never touches a network.
func New(opts ...Option) *Fake {
	f := &Fake{
		now:        time.Now,
		failNext:   map[string]kafka.Kind{},
		failAlways: map[string]kafka.Kind{},
		latency:    map[string]time.Duration{},
		model:      model{clusterID: "fake-cluster"},
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// record appends one call to the log (TECH-SPEC §4.3 Recording row).
func (f *Fake) record(method string, mutating bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, CallRecord{Method: method, Mutating: mutating, Err: err})
}

// invoke is the single path every port method runs through: it applies
// Unreachable, Latency (respecting ctx), the injected context deadline check
// (TECH-SPEC L3), and FailNext/FailAlways, then records the outcome. fn only
// runs when none of those short-circuit the call.
//
// invoke is a free function, not a method, because Go methods cannot carry
// their own type parameters.
func invoke[T any](f *Fake, ctx context.Context, method string, mutating bool, fn func() (T, error)) (T, error) {
	var zero T

	f.mu.Lock()
	unreachable := f.unreachable
	lat := f.latency[method]
	kind, hasFault := f.failNext[method]
	if hasFault {
		delete(f.failNext, method)
	} else {
		kind, hasFault = f.failAlways[method]
	}
	f.mu.Unlock()

	if unreachable {
		err := &kafka.Error{Kind: kafka.KindUnavailable, Cause: errors.New("fake: unreachable")}
		f.record(method, mutating, err)
		return zero, err
	}

	if lat > 0 {
		timer := time.NewTimer(lat)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			err := &kafka.Error{Kind: kafka.KindTimeout, Cause: ctx.Err()}
			f.record(method, mutating, err)
			return zero, err
		}
	}

	if err := ctx.Err(); err != nil {
		wrapped := &kafka.Error{Kind: kafka.KindTimeout, Cause: err}
		f.record(method, mutating, wrapped)
		return zero, wrapped
	}

	if hasFault {
		err := &kafka.Error{Kind: kind, Cause: fmt.Errorf("fake: injected %s failure", method)}
		f.record(method, mutating, err)
		return zero, err
	}

	result, err := fn()
	f.record(method, mutating, err)
	return result, err
}
