package message

import (
	"context"
	"errors"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// errCodeOf names err for a RESULT event's Error.Code (FUNC-SPEC §8.5): a
// *kafka.Error reports its Kind, anything else "INTERNAL".
func errCodeOf(err error) string {
	var ke *kafka.Error
	if errors.As(err, &ke) {
		return ke.Kind.String()
	}
	return "INTERNAL"
}

// newEvent builds the base audit.Event for one M1-M7 command invocation
// (FUNC-SPEC §8.5): its ATTEMPT and RESULT (or, for a gate rejection or a
// break-glass read, its one RESULT) share this EventID and Target/Caller/
// BreakGlass. commandID must name an M1-M7 command.Table entry.
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
// the same EventID, Target, Caller, and BreakGlass, a fresh Timestamp and
// DurationMs measured from attempt's, and severity from outcome
// (audit.SeverityFor). errCode, when non-empty, populates Event.Error.
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
// before anything else in Produce/ProduceBulk/Tombstone/Read/Search/
// Filter/Get runs. On rejection it emits a single REJECTED RESULT audit
// event via core.CheckAudited — no ATTEMPT, since the command never ran —
// and returns the *core.PolicyError.
func (s *Service) checkGate(ctx context.Context, caller core.Caller, commandID, topic string) error {
	desc, _ := command.Lookup(commandID)
	return core.CheckAudited(ctx, s.runner, s.auditor, caller, desc, s.newEvent(caller, commandID, topic))
}

// auditBreakGlassRead emits a single HIGH RESULT event for a successful
// M1-M4 read whose caller presented a break-glass reason (FUNC-SPEC §9.5):
// reads are otherwise entirely unaudited (FUNC-SPEC §8.5 scope) — this is
// the one exception, and it fires whenever the reason is present, whether
// or not the lock this call passed through was actually engaged.
func (s *Service) auditBreakGlassRead(ctx context.Context, caller core.Caller, commandID, topic string) {
	if caller.BreakGlassReason == "" {
		return
	}
	attempt := s.newEvent(caller, commandID, topic)
	s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeSucceeded, ""))
}

// resultEvent derives attempt's RESULT counterpart from a bulk operation's
// summary (FUNC-SPEC §9.4): Failed when any item failed, Succeeded on a
// clean sweep — Kafka admin operations cannot roll back, so a bulk RESULT is
// never itself "partial," matching the 200-vs-207 HTTP split which carries
// the per-item detail instead.
func (s *Service) resultEvent(attempt audit.Event, summary core.BulkSummary) audit.Event {
	outcome := audit.OutcomeSucceeded
	if summary.Failed > 0 {
		outcome = audit.OutcomeFailed
	}
	return s.resultEventOutcome(attempt, outcome, "")
}
