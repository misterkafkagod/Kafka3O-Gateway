package audit_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
)

func TestAuditEvent_SchemaFieldsMatchSpec(t *testing.T) {
	t.Parallel()
	keyID := "k1"
	duration := int64(42)
	ev := audit.Event{
		EventID: "e1", Timestamp: time.Now(), RequestID: "r1",
		Phase: audit.PhaseResult, Severity: audit.SeverityHigh,
		CommandID: "M5", CommandName: "Produce messages",
		Target: audit.Target{Type: "topic", Name: "t", Partitions: []int32{0}},
		Caller: audit.Caller{KeyID: &keyID, Tier: "operator", ClientIP: "127.0.0.1"},
		DryRun: false, BreakGlass: &audit.BreakGlass{Reason: "incident-1"},
		Outcome: audit.OutcomeSucceeded, Error: &audit.EventError{Code: "KAFKA_ERROR"},
		DurationMs: &duration,
	}

	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	want := []string{
		"eventId", "timestamp", "requestId", "phase", "severity",
		"commandId", "commandName", "target", "caller", "dryRun",
		"breakGlass", "outcome", "error", "durationMs",
	}
	for _, k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("missing key %q in marshaled event: %v", k, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("marshaled event has %d keys, want %d: %v", len(got), len(want), got)
	}
}

func TestAuditor_SeverityTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		commandID  string
		outcome    audit.Outcome
		breakGlass bool
		want       audit.Severity
	}{
		{"successful execution is info", "M1", audit.OutcomeSucceeded, false, audit.SeverityInfo},
		{"rejected is warn", "M5", audit.OutcomeRejected, false, audit.SeverityWarn},
		{"failed is warn", "M5", audit.OutcomeFailed, false, audit.SeverityWarn},
		{"break-glass is high even on reject", "M1", audit.OutcomeRejected, true, audit.SeverityHigh},
		{"break-glass is high on success", "M1", audit.OutcomeSucceeded, true, audit.SeverityHigh},
		{"T7 success is high", "T7", audit.OutcomeSucceeded, false, audit.SeverityHigh},
		{"T8 success is high", "T8", audit.OutcomeSucceeded, false, audit.SeverityHigh},
		{"T11 success is high", "T11", audit.OutcomeSucceeded, false, audit.SeverityHigh},
		{"T12 success is high", "T12", audit.OutcomeSucceeded, false, audit.SeverityHigh},
		{"G5 success is high", "G5", audit.OutcomeSucceeded, false, audit.SeverityHigh},
		{"T7 rejection is warn, not high", "T7", audit.OutcomeRejected, false, audit.SeverityWarn},
		{"an ordinary successful W command is not high", "T5", audit.OutcomeSucceeded, false, audit.SeverityInfo},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := audit.SeverityFor(tc.commandID, tc.outcome, tc.breakGlass)
			if got != tc.want {
				t.Errorf("SeverityFor(%s, %s, breakGlass=%v) = %s, want %s", tc.commandID, tc.outcome, tc.breakGlass, got, tc.want)
			}
		})
	}
}

func TestAuditor_CallerKeyIDNilOn401AndAuthDisabled(t *testing.T) {
	t.Parallel()
	// A 401 rejection or auth-disabled request has no resolved key: Caller
	// carries a nil KeyID, which must marshal to JSON null, never be
	// omitted (TECH-SPEC C14).
	ev := audit.Event{Caller: audit.Caller{KeyID: nil, Tier: "operator"}}

	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}
	caller, ok := got["caller"].(map[string]any)
	if !ok {
		t.Fatalf("caller field missing or wrong type: %v", got)
	}
	if v, present := caller["keyId"]; !present || v != nil {
		t.Errorf("caller.keyId = %v (present=%v), want present and null", v, present)
	}
	if caller["tier"] != "operator" {
		t.Errorf("caller.tier = %v, want operator", caller["tier"])
	}
}
