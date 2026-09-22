package audit

import (
	"context"
	"fmt"
)

// Auditor sequences the two-phase audit protocol (FUNC-SPEC §8.5): Attempt
// before a mutation touches Kafka, Result after — or Result alone for a
// dry-run or a gate rejection, since nothing ever ran. sinks is every
// configured sink (the stdout/file sink is always one of them; a Kafka sink
// is appended only when configured — FUNC-SPEC F5). If any sink fails on
// Attempt, the whole call fails closed (FUNC-SPEC §8.5 V2) — Attempt does
// not distinguish which sink failed, since the guarantee is "every
// configured sink recorded this before the mutation runs," not "the Kafka
// sink specifically." Result failures never block: the mutation already
// happened by the time Result fires.
type Auditor struct {
	sinks []Sink
}

// NewAuditor builds an Auditor writing to every one of sinks, in order.
func NewAuditor(sinks ...Sink) *Auditor {
	return &Auditor{sinks: sinks}
}

// Attempt writes ev (with Phase forced to PhaseAttempt) to every sink,
// returning the first error encountered (FUNC-SPEC §8.5 V2 fail-closed).
func (a *Auditor) Attempt(ctx context.Context, ev Event) error {
	ev.Phase = PhaseAttempt
	for _, sink := range a.sinks {
		if err := sink.Write(ctx, ev); err != nil {
			return fmt.Errorf("audit: attempt: %w", err)
		}
	}
	return nil
}

// Result writes ev (with Phase forced to PhaseResult) to every sink. A sink
// failure here is not reported to the caller — the mutation this event
// describes has already happened (or was never attempted, for a dry-run or
// rejection); there is nothing left to fail closed.
func (a *Auditor) Result(ctx context.Context, ev Event) {
	ev.Phase = PhaseResult
	for _, sink := range a.sinks {
		_ = sink.Write(ctx, ev)
	}
}
