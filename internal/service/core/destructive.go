package core

import (
	"context"
	"errors"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Plan is what a destructive command's plan step returns: enough to build
// the dry-run response (FUNC-SPEC §8.3) and to check confirm against
// ConfirmTarget() (FUNC-SPEC §8.6).
type Plan interface {
	// ConfirmTarget returns what the caller's confirm field must equal: the
	// target's own name/id for a single-target command (T7, T9, T10, T11,
	// T12, G4-G7, C5, S1, S2), or a plan token for a multi-target one (T8,
	// C9, C12 — built by a later task's plantoken.Token).
	ConfirmTarget() string
}

// Result is Destructive's return value: either a dry-run plan (DryRun true,
// Value the zero T) or an executed command's result (DryRun false, Value
// the apply result) — FUNC-SPEC §8.3's dry-run envelope and a normal
// response share this one type so Destructive itself never needs to know
// which the caller will render.
type Result[P Plan, T any] struct {
	DryRun bool
	Plan   P
	Value  T
}

// Destructive sequences FUNC-SPEC §9.1's lower half for one destructive
// command (§5.6): gate check (F1-F3, via gates.Check, reusing CheckAudited's
// already-tested REJECTED-audit behavior) -> plan -> confirm check -> a
// dry-run short-circuit (a single INFO RESULT, no ATTEMPT, no apply) ->
// ATTEMPT (fail-closed, FUNC-SPEC §8.5 V2) -> apply -> RESULT (FAILED on
// error). It never touches a Kafka port itself — plan and apply are the
// caller's own closures — so it is the one place every destructive
// command's confirm/dryRun/audit sequencing lives (TECH-SPEC §2.3, O2), not
// duplicated per command.
//
// attempt is the base audit.Event the caller has already built (EventID,
// Target, Caller, CommandID, ...), exactly as message.Service.newEvent
// builds one for M5-M7 — Destructive only ever derives DryRun/Outcome/
// Severity/Error from it, never Target/Caller/CommandID (building those is
// the caller's own job: audit depends on neither core nor command, so it
// cannot build one itself).
//
// A plan() failure (e.g. the target doesn't exist) returns before any
// confirm check or audit event, the same as a bulk validation failure
// (FUNC-SPEC §9.4): nothing has been decided about this call yet, so there
// is nothing to report. A confirm mismatch is the same — FUNC-SPEC §8.5
// only promises an audit line for a gate rejection or an executed/dry-run
// outcome, not for a caller simply getting the confirmation wrong.
func Destructive[P Plan, T any](
	ctx context.Context,
	r Runner,
	auditor *audit.Auditor,
	caller Caller,
	descriptor command.Descriptor,
	attempt audit.Event,
	confirm string,
	dryRun bool,
	plan func() (P, error),
	apply func(P) (T, error),
) (Result[P, T], error) {
	var zero Result[P, T]

	if err := CheckAudited(ctx, r, auditor, caller, descriptor, attempt); err != nil {
		return zero, err
	}

	p, err := plan()
	if err != nil {
		return zero, err
	}

	if confirm != p.ConfirmTarget() {
		return zero, &PolicyError{Code: ConfirmationMismatch, Details: map[string]any{"plan": p}}
	}

	attempt.DryRun = dryRun

	if dryRun {
		auditor.Result(ctx, resultEvent(attempt, audit.OutcomeSucceeded, ""))
		return Result[P, T]{DryRun: true, Plan: p}, nil
	}

	if err := auditor.Attempt(ctx, attempt); err != nil {
		return zero, &PolicyError{Code: AuditUnavailable, Message: err.Error()}
	}

	value, err := apply(p)
	if err != nil {
		auditor.Result(ctx, resultEvent(attempt, audit.OutcomeFailed, destructiveErrCode(err)))
		return Result[P, T]{Plan: p}, err
	}

	auditor.Result(ctx, resultEvent(attempt, audit.OutcomeSucceeded, ""))
	return Result[P, T]{Plan: p, Value: value}, nil
}

// resultEvent derives attempt's RESULT counterpart (FUNC-SPEC §8.5): the
// same EventID, Target, Caller, BreakGlass, and DryRun, a fresh Timestamp
// and DurationMs measured from attempt's (the real wall clock — Destructive
// is a free function with no injected clock of its own, unlike a Service),
// and severity from outcome (audit.SeverityFor). errCode, when non-empty,
// populates Event.Error.
func resultEvent(attempt audit.Event, outcome audit.Outcome, errCode string) audit.Event {
	result := attempt
	result.Timestamp = time.Now()
	result.Outcome = outcome
	result.Severity = audit.SeverityFor(attempt.CommandID, outcome, attempt.BreakGlass != nil)
	d := result.Timestamp.Sub(attempt.Timestamp).Milliseconds()
	result.DurationMs = &d
	if errCode != "" {
		result.Error = &audit.EventError{Code: errCode}
	}
	return result
}

// destructiveErrCode names apply's error for a RESULT event's Error.Code
// (FUNC-SPEC §8.5): a *PolicyError reports its own Code, a *kafka.Error its
// Kind, anything else "INTERNAL".
func destructiveErrCode(err error) string {
	if code, ok := CodeOf(err); ok {
		return code.String()
	}
	var ke *kafka.Error
	if errors.As(err, &ke) {
		return ke.Kind.String()
	}
	return "INTERNAL"
}
