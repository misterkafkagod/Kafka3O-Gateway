package audit_test

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// stubProducer is a minimal kafka.Producer test double local to this
// package's tests (audit-tests' depguard rule allows internal/kafka but not
// internal/kafka/fake, so audit's own tests build their own tiny stub rather
// than importing the shared fake).
type stubProducer struct {
	mu       sync.Mutex
	calls    int
	produced []kafka.ProduceRequest
	failNext bool
	latency  time.Duration
}

func (p *stubProducer) Produce(ctx context.Context, _ string, records []kafka.ProduceRequest) ([]kafka.ProduceResult, error) {
	p.mu.Lock()
	p.calls++
	fail := p.failNext
	p.failNext = false
	lat := p.latency
	p.mu.Unlock()

	if lat > 0 {
		select {
		case <-time.After(lat):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errors.New("stubProducer: injected failure")
	}

	p.mu.Lock()
	p.produced = append(p.produced, records...)
	p.mu.Unlock()

	return make([]kafka.ProduceResult, len(records)), nil
}

// Calls returns the number of times Produce has been called so far.
func (p *stubProducer) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}
