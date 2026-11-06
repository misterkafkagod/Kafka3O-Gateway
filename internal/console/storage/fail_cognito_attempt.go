package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) FailCognitoAttempt(ctx context.Context, failure cognito.AttemptFailure) error {
	if failure.OperationID == "" || failure.EventID == failure.OperationID {
		return ErrInvalidAuditEvent
	}
	return d.AppendAuditEvent(ctx, AuditEvent{
		EventID: failure.EventID, OperationID: failure.OperationID, OccurredAt: failure.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.callback",
		Decision: "denied", Outcome: AuditFailed,
	})
}
