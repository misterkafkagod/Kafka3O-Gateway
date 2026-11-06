package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) FailBootstrapAttempt(ctx context.Context, failure identity.BootstrapFailure) error {
	if failure.OperationID == "" || failure.EventID == failure.OperationID {
		return ErrInvalidAuditEvent
	}
	event := AuditEvent{
		EventID: failure.EventID, OperationID: failure.OperationID, OccurredAt: failure.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.bootstrap",
		Outcome: AuditFailed,
	}
	if failure.Denied {
		event.Decision = "denied"
	}
	return d.AppendAuditEvent(ctx, event)
}
