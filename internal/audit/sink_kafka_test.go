package audit_test

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
)

func TestKafkaSink_BoundedByCtxAndReturnsError(t *testing.T) {
	t.Parallel()
	p := &stubProducer{latency: 200 * time.Millisecond}
	sink := audit.NewKafkaSink(p, "audit-topic")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sink.Write(ctx, audit.Event{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Write() with an expiring ctx returned nil error")
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("Write() took %s, want it bounded near the 20ms ctx deadline, not the full 200ms latency", elapsed)
	}
}

func TestKafkaSink_UsesDedicatedProducer(t *testing.T) {
	t.Parallel()
	dataPlane := &stubProducer{}
	auditProducer := &stubProducer{}
	sink := audit.NewKafkaSink(auditProducer, "audit-topic")

	if err := sink.Write(context.Background(), audit.Event{EventID: "e1"}); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	if auditProducer.Calls() != 1 {
		t.Errorf("audit producer calls = %d, want 1", auditProducer.Calls())
	}
	if dataPlane.Calls() != 0 {
		t.Errorf("data-plane producer calls = %d, want 0 (the audit sink never touches it -- TECH-SPEC C5)", dataPlane.Calls())
	}
}
