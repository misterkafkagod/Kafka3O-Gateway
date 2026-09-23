package security

import (
	"context"
	"errors"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// errCodeOf names err for a RESULT event's Error.Code (FUNC-SPEC §8.5): a
// *core.PolicyError reports its own Code, a *kafka.Error its Kind, anything
// else "INTERNAL".
func errCodeOf(err error) string {
	if code, ok := core.CodeOf(err); ok {
		return code.String()
	}
	var ke *kafka.Error
	if errors.As(err, &ke) {
		return ke.Kind.String()
	}
	return "INTERNAL"
}

// newEvent builds the base audit.Event for one S1 or S2 command invocation
// (FUNC-SPEC §8.5): its ATTEMPT and RESULT (or, for a gate rejection or a
// dry-run, its one RESULT) share this EventID and Target/Caller/BreakGlass.
// commandID must name S1 or S2; target is the username (S1) or the entity
// descriptor (S2) — never a password (FUNC-SPEC §8.2, TECH-SPEC C7: "S1
// passwords never appear in audit target").
func (s *Service) newEvent(caller core.Caller, commandID, target string) audit.Event {
	desc, _ := command.Lookup(commandID)

	var keyID *string
	if caller.KeyID != nil {
		id := *caller.KeyID
		keyID = &id
	}
	var breakGlass *audit.BreakGlass
	if caller.BreakGlassReason != "" {
		breakGlass = &audit.BreakGlass{Reason: caller.BreakGlassReason}
	}

	targetType := "user"
	if commandID == "S2" {
		targetType = "quota"
	}

	return audit.Event{
		EventID:     s.newEventID(),
		Timestamp:   s.now(),
		RequestID:   caller.RequestID,
		CommandID:   desc.ID,
		CommandName: desc.Name,
		Target:      audit.Target{Type: targetType, Name: target},
		Caller:      audit.Caller{KeyID: keyID, Tier: caller.Tier, ClientIP: caller.ClientIP},
		BreakGlass:  breakGlass,
	}
}

// resultEventOutcome derives attempt's RESULT counterpart (FUNC-SPEC §8.5):
// the same EventID, Target, Caller, BreakGlass, and DryRun, a fresh
// Timestamp and DurationMs measured from attempt's, and severity from
// outcome (audit.SeverityFor). errCode, when non-empty, populates
// Event.Error.
func (s *Service) resultEventOutcome(attempt audit.Event, outcome audit.Outcome, errCode string) audit.Event {
	result := attempt
	result.Timestamp = s.now()
	result.Outcome = outcome
	result.Severity = audit.SeverityFor(attempt.CommandID, outcome, attempt.BreakGlass != nil)
	d := result.Timestamp.Sub(attempt.Timestamp).Milliseconds()
	result.DurationMs = &d
	if errCode != "" {
		result.Error = &audit.EventError{Code: errCode}
	}
	return result
}

// checkGate runs gates.Check for commandID (FUNC-SPEC §9.1 nodes F-K3),
// before anything else in Create runs. On rejection it emits a single
// REJECTED RESULT audit event via core.CheckAudited — no ATTEMPT, since the
// command never ran — and returns the *core.PolicyError. S1 delete and S2
// alter (FUNC-SPEC §5.6, destructive) run this same check as part of
// core.Destructive instead, not via this helper.
func (s *Service) checkGate(ctx context.Context, caller core.Caller, commandID, target string) error {
	desc, _ := command.Lookup(commandID)
	return core.CheckAudited(ctx, s.runner, s.auditor, caller, desc, s.newEvent(caller, commandID, target))
}
