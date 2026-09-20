package core

import (
	"context"

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
