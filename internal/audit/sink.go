package audit

import "context"

// Sink writes one audit event (TECH-SPEC I3). Lifecycle (Close) belongs to
// the concrete implementation, owned by the composition root.
type Sink interface {
	Write(ctx context.Context, ev Event) error
}
