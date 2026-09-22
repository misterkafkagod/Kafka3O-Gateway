package topic

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

// newEvent builds the base audit.Event for one T5/T6/T9/T10 command
// invocation (FUNC-SPEC §8.5): its ATTEMPT and RESULT (or, for a gate
// rejection or a dry-run, its one RESULT) share this EventID and Target/
// Caller/BreakGlass. commandID must name a T5/T6/T9/T10 command.Table
// entry; topic is "" for T6, which targets many topics at once.
func (s *Service) newEvent(caller core.Caller, commandID, topic string) audit.Event {
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

	return audit.Event{
		EventID:     s.newEventID(),
		Timestamp:   s.now(),
		RequestID:   caller.RequestID,
		CommandID:   desc.ID,
		CommandName: desc.Name,
		Target:      audit.Target{Type: "topic", Name: topic},
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

// resultEvent derives attempt's RESULT counterpart from a bulk operation's
// summary (FUNC-SPEC §9.4): Failed when any item failed, Succeeded on a
// clean sweep — Kafka admin operations cannot roll back, so a bulk RESULT
// is never itself "partial," matching the 200-vs-207 HTTP split which
// carries the per-item detail instead.
func (s *Service) resultEvent(attempt audit.Event, summary core.BulkSummary) audit.Event {
	outcome := audit.OutcomeSucceeded
	if summary.Failed > 0 {
		outcome = audit.OutcomeFailed
	}
	return s.resultEventOutcome(attempt, outcome, "")
}

// checkGate runs gates.Check for commandID (FUNC-SPEC §9.1 nodes F-K3),
// before anything else in Create/CreateBulk runs. On rejection it emits a
// single REJECTED RESULT audit event via core.CheckAudited — no ATTEMPT,
// since the command never ran — and returns the *core.PolicyError. T9 and
// T10 (FUNC-SPEC §5.6, destructive) run this same check as part of
// core.Destructive instead, not via this helper.
func (s *Service) checkGate(ctx context.Context, caller core.Caller, commandID, topic string) error {
	desc, _ := command.Lookup(commandID)
	return core.CheckAudited(ctx, s.runner, s.auditor, caller, desc, s.newEvent(caller, commandID, topic))
}
