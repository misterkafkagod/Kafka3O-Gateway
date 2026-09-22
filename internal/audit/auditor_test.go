package audit_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
)

func TestAuditor_AttemptThenResultOrdering(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	a := audit.NewAuditor(rec)

	ev := audit.Event{EventID: "e1", CommandID: "M5"}
	if err := a.Attempt(context.Background(), ev); err != nil {
		t.Fatalf("Attempt() error: %v", err)
	}
	a.Result(context.Background(), ev)

	events := rec.Events()
	if len(events) != 2 {
		t.Fatalf("Events() = %d, want 2", len(events))
	}
	if events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Errorf("phases = [%s, %s], want [ATTEMPT, RESULT]", events[0].Phase, events[1].Phase)
	}
}

func TestAuditor_DryRunEmitsSingleResult(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	a := audit.NewAuditor(rec)

	a.Result(context.Background(), audit.Event{EventID: "e1", DryRun: true, Outcome: audit.OutcomeSucceeded})

	events := rec.Events()
	if len(events) != 1 || events[0].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want exactly one RESULT event", events)
	}
}

func TestAuditor_AttemptSinkFailureReturnsError_Recording(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	rec.FailNext(audit.PhaseAttempt)
	a := audit.NewAuditor(rec)

	if err := a.Attempt(context.Background(), audit.Event{}); err == nil {
		t.Fatal("Attempt() with a failing recording sink returned nil error")
	}
}

func TestAuditor_AttemptSinkFailureReturnsError_Kafka(t *testing.T) {
	t.Parallel()
	p := &stubProducer{failNext: true}
	sink := audit.NewKafkaSink(p, "audit-topic")
	a := audit.NewAuditor(sink)

	if err := a.Attempt(context.Background(), audit.Event{}); err == nil {
		t.Fatal("Attempt() with a failing kafka sink returned nil error")
	}
}

func TestAuditor_AttemptSinkFailureReturnsError_Slog(t *testing.T) {
	t.Parallel()
	// SlogSink's own Write never errors -- pairing a working SlogSink with a
	// failing sink later in the list still fails Attempt closed, proving
	// the mechanism doesn't depend on which configured sink failed
	// (FUNC-SPEC §8.5 V2).
	rec := audittest.New()
	rec.FailNext(audit.PhaseAttempt)
	slogSink := audit.NewSlogSink(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	a := audit.NewAuditor(slogSink, rec)

	if err := a.Attempt(context.Background(), audit.Event{}); err == nil {
		t.Fatal("Attempt() with a failing sink after a working SlogSink returned nil error")
	}
}

func TestAuditor_Race(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	a := audit.NewAuditor(rec)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := audit.Event{EventID: "e"}
			_ = a.Attempt(context.Background(), ev)
			a.Result(context.Background(), ev)
		}()
	}
	wg.Wait()

	if got := len(rec.Events()); got != 40 {
		t.Errorf("Events() = %d, want 40", got)
	}
}
