package message_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// TestRun_BreakGlassReadSuccessEmitsResultHighWithReason proves a
// successful M1-M4 read whose caller presented a break-glass reason emits
// exactly one HIGH RESULT audit event carrying it (FUNC-SPEC §9.5) — reads
// are otherwise entirely unaudited (FUNC-SPEC §8.5 scope).
func TestRun_BreakGlassReadSuccessEmitsResultHighWithReason(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	caller := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}
	_, err := svc.Read(context.Background(), caller, message.ReadParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning},
	})
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}

	events := rec.Events()
	if len(events) != 1 || events[0].Phase != audit.PhaseResult || events[0].Outcome != audit.OutcomeSucceeded {
		t.Fatalf("events = %+v, want exactly one SUCCEEDED RESULT", events)
	}
	if events[0].Severity != audit.SeverityHigh {
		t.Errorf("Severity = %s, want HIGH", events[0].Severity)
	}
	if events[0].BreakGlass == nil || events[0].BreakGlass.Reason != "incident-1" {
		t.Errorf("BreakGlass = %+v, want reason incident-1", events[0].BreakGlass)
	}
	if events[0].CommandID != "M1" {
		t.Errorf("CommandID = %q, want M1", events[0].CommandID)
	}
}

// TestMessageService_Read_NoBreakGlassReasonIsUnaudited proves the ordinary
// (no break-glass) read path stays entirely unaudited, the FUNC-SPEC §8.5
// baseline this test file's HIGH-event case is the one exception to.
func TestMessageService_Read_NoBreakGlassReasonIsUnaudited(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	_, err := svc.Read(context.Background(), core.Caller{Tier: core.TierOperator}, message.ReadParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning},
	})
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}

	if events := rec.Events(); len(events) != 0 {
		t.Errorf("events = %+v, want none", events)
	}
}
