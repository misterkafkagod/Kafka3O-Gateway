// Package audittest is the audit subsystem's test double (TECH-SPEC §4.4):
// a Sink that records every Write and supports fault injection, so tests
// across internal/service and internal/api can assert on audit behaviour
// without a real sink. It is test-only — imported exclusively from
// _test.go files (TECH-SPEC P3, enforced by depguard).
package audittest

import (
	"context"
	"fmt"
	"sync"

	"github.com/misterkafkagod/kafka3o/internal/audit"
)

// RecordingSink is an audit.Sink test double: it records every event that
// wrote successfully and supports fault injection per phase.
type RecordingSink struct {
	mu         sync.Mutex
	events     []audit.Event
	failNext   map[audit.Phase]bool
	failAlways map[audit.Phase]bool
}

// New builds an empty RecordingSink.
func New() *RecordingSink {
	return &RecordingSink{
		failNext:   map[audit.Phase]bool{},
		failAlways: map[audit.Phase]bool{},
	}
}

// Write records ev, unless a fault is injected for ev.Phase.
func (s *RecordingSink) Write(_ context.Context, ev audit.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failNext[ev.Phase] {
		delete(s.failNext, ev.Phase)
		return fmt.Errorf("audittest: injected %s failure", ev.Phase)
	}
	if s.failAlways[ev.Phase] {
		return fmt.Errorf("audittest: injected %s failure", ev.Phase)
	}

	s.events = append(s.events, ev)
	return nil
}

// Events returns every successfully recorded event, in order.
func (s *RecordingSink) Events() []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]audit.Event, len(s.events))
	copy(out, s.events)
	return out
}

// FailNext makes the next Write for phase fail once, then clears itself.
func (s *RecordingSink) FailNext(phase audit.Phase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext[phase] = true
}

// FailAlways makes every future Write for phase fail.
func (s *RecordingSink) FailAlways(phase audit.Phase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAlways[phase] = true
}

// compile-time proof that RecordingSink satisfies audit.Sink.
var _ audit.Sink = (*RecordingSink)(nil)
