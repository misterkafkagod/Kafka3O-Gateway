package core

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
)

// CheckFunc is the shape of gates.Check (TECH-SPEC §2.3). core cannot import
// internal/service/gates directly: gates imports core for the Caller,
// Policy, and PolicyError types, so core importing gates back would be a
// dependency cycle. Whoever constructs a Runner supplies gates.Check as this
// field instead — the same dependency-inversion shape already used for the
// Kafka port (TECH-SPEC §2.2) — so core stays free of any gates import while
// run still enforces the identical check.
type CheckFunc func(caller Caller, descriptor command.Descriptor, policy Policy) error

// Runner carries what run needs on every call: the Policy gates.Check
// evaluates, and the Check function itself (TECH-SPEC §2.3).
type Runner struct {
	Policy Policy
	Check  CheckFunc
}

// run calls r.Check(caller, descriptor, r.Policy) before invoking fn, so fn
// never executes when a gate rejects the call (TECH-SPEC §2.3, "gate check
// as one function").
//
// run is a free function, not a method on Runner, because Go methods cannot
// carry their own type parameters; every service method calls it as
// run(s.runner, ctx, caller, descriptor, fn).
func run[T any](r Runner, ctx context.Context, caller Caller, descriptor command.Descriptor, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := r.Check(caller, descriptor, r.Policy); err != nil {
		return zero, err
	}
	return fn(ctx)
}

// CheckAudited is run's gate check alone (no fn), extended to emit a
// REJECTED RESULT audit event on rejection (FUNC-SPEC §9.1 rules): WARN
// normally, HIGH when break-glass was attempted (FUNC-SPEC §8.5 V6). attempt
// is the event the caller has already built for this call (EventID, Target,
// Caller, CommandID, ...) — building it is the caller's own job (audit
// depends on neither core nor command, so it cannot build one itself);
// CheckAudited only overwrites Outcome/Severity/Error before handing it to
// auditor.Result, exactly as any other RESULT event is derived from its
// ATTEMPT. A passing check returns nil without touching auditor at all — the
// caller's own success path owns that event.
func CheckAudited(ctx context.Context, r Runner, auditor *audit.Auditor, caller Caller, descriptor command.Descriptor, attempt audit.Event) error {
	err := r.Check(caller, descriptor, r.Policy)
	if err == nil {
		return nil
	}

	code, _ := CodeOf(err)
	result := attempt
	result.Outcome = audit.OutcomeRejected
	result.Severity = audit.SeverityFor(descriptor.ID, audit.OutcomeRejected, caller.BreakGlassReason != "")
	result.Error = &audit.EventError{Code: code.String()}
	auditor.Result(ctx, result)
	return err
}
