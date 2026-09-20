// Package gates implements FUNC-SPEC §9.1 nodes F through K3: the upper half
// of the request pipeline, run once per request before validation (node L)
// and before any Kafka call. Every service method reaches Check through
// internal/service/core's run helper (TECH-SPEC §2.3); Check itself never
// touches Kafka, audit, or HTTP.
package gates

import (
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Check evaluates FUNC-SPEC §9.1 nodes F-K3 for one request:
//
//   - F/G: a W command requires an operator-tier caller, else 403 TierForbidden.
//   - H: an operator is still blocked while read-only mode is on, 403 ReadOnlyMode.
//   - I: a W command whose id is administratively disabled is blocked, 403 OperationDisabled.
//   - J/K/K2: a data-plane command (M1-M8, descriptor.DataPlane) is blocked
//     while the lock is on, unless the caller is an operator carrying a
//     non-empty break-glass reason — that combination passes (K3). Break-
//     glass bypasses the lock (F6) only: it is checked after, and
//     independently of, the F2/F3 checks above, so it can never bypass
//     read-only mode or a disabled operation (FUNC-SPEC §9.1 rules).
//
// Check never mutates its arguments and never calls anything outside this
// function (TECH-SPEC O1: gates key off command.Descriptor fields, never off
// command ids — there is no switch on descriptor.ID here).
func Check(caller core.Caller, descriptor command.Descriptor, policy core.Policy) error {
	if descriptor.Access == command.W {
		if caller.Tier != core.TierOperator {
			return &core.PolicyError{Code: core.TierForbidden}
		}
		if policy.ReadOnly {
			return &core.PolicyError{Code: core.ReadOnlyMode}
		}
		if policy.Disabled[descriptor.ID] {
			return &core.PolicyError{Code: core.OperationDisabled}
		}
	}

	if descriptor.DataPlane && policy.DataPlaneLock {
		if caller.BreakGlassReason == "" || caller.Tier != core.TierOperator {
			return &core.PolicyError{Code: core.DataPlaneLocked}
		}
	}

	return nil
}

// compile-time proof that Check satisfies core.CheckFunc.
var _ core.CheckFunc = Check
