package core_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestRun_WRejectionEmitsResultRejectedWarn(t *testing.T) {
	t.Parallel()
	codes := []core.Code{core.TierForbidden, core.ReadOnlyMode, core.OperationDisabled, core.DataPlaneLocked}
	for _, code := range codes {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			rec := audittest.New()
			auditor := audit.NewAuditor(rec)
			runner := core.Runner{Check: func(core.Caller, command.Descriptor, core.Policy) error {
				return &core.PolicyError{Code: code}
			}}
			attempt := audit.Event{EventID: "e1", CommandID: "T7"}

			err := core.CheckAudited(context.Background(), runner, auditor,
				core.Caller{Tier: core.TierReader}, command.Descriptor{ID: "T7", Access: command.W}, attempt)

			if !core.IsCode(err, code) {
				t.Fatalf("CheckAudited() error = %v, want Code %s", err, code)
			}
			events := rec.Events()
			if len(events) != 1 {
				t.Fatalf("Events() = %+v, want exactly one", events)
			}
			if events[0].Phase != audit.PhaseResult || events[0].Outcome != audit.OutcomeRejected {
				t.Errorf("event = %+v, want Phase RESULT Outcome REJECTED", events[0])
			}
			if events[0].Severity != audit.SeverityWarn {
				t.Errorf("Severity = %s, want WARN", events[0].Severity)
			}
			if events[0].Error == nil || events[0].Error.Code != code.String() {
				t.Errorf("Error = %+v, want Code %s", events[0].Error, code)
			}
		})
	}
}

func TestRun_BreakGlassAttemptByReaderEmitsResultRejectedHigh(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	runner := core.Runner{Check: func(core.Caller, command.Descriptor, core.Policy) error {
		return &core.PolicyError{Code: core.DataPlaneLocked}
	}}
	caller := core.Caller{Tier: core.TierReader, BreakGlassReason: "incident-1"}
	// attempt.BreakGlass is the caller's own job to set (CheckAudited only
	// ever overwrites Outcome/Severity/Error) — this mirrors exactly what
	// message.Service.newEvent builds for a caller presenting the header.
	attempt := audit.Event{EventID: "e1", CommandID: "M1", BreakGlass: &audit.BreakGlass{Reason: "incident-1"}}

	err := core.CheckAudited(context.Background(), runner, auditor, caller,
		command.Descriptor{ID: "M1", Access: command.R, DataPlane: true}, attempt)

	if !core.IsCode(err, core.DataPlaneLocked) {
		t.Fatalf("CheckAudited() error = %v, want Code DataPlaneLocked", err)
	}
	events := rec.Events()
	if len(events) != 1 || events[0].Severity != audit.SeverityHigh {
		t.Fatalf("events = %+v, want one HIGH event (break-glass was attempted)", events)
	}
	if events[0].BreakGlass == nil || events[0].BreakGlass.Reason != "incident-1" {
		t.Errorf("BreakGlass = %+v, want reason incident-1", events[0].BreakGlass)
	}
}

// TestRun_BreakGlassDoesNotBypassReadOnly proves CheckAudited faithfully
// reports whatever the gate decided, never second-guessing a ReadOnlyMode
// rejection into a pass just because break-glass was present (FUNC-SPEC
// §9.1 rules C1: break-glass bypasses F6 only). gates.Check's own decision
// for this exact scenario is separately proven in
// internal/service/gates.TestGatesCheck_BreakGlassBypassesF6Only — this
// test is about CheckAudited's audit wiring, not re-deriving that logic.
func TestRun_BreakGlassDoesNotBypassReadOnly(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	runner := core.Runner{Check: func(core.Caller, command.Descriptor, core.Policy) error {
		return &core.PolicyError{Code: core.ReadOnlyMode}
	}}
	caller := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}
	attempt := audit.Event{EventID: "e1", CommandID: "M5"}

	err := core.CheckAudited(context.Background(), runner, auditor, caller,
		command.Descriptor{ID: "M5", Access: command.W, DataPlane: true}, attempt)

	if !core.IsCode(err, core.ReadOnlyMode) {
		t.Fatalf("CheckAudited() error = %v, want Code ReadOnlyMode (break-glass must not bypass F2)", err)
	}
}
