package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) FailLocalPasswordReset(ctx context.Context, failure identity.LocalPasswordResetFailure) (result error) {
	event := AuditEvent{
		EventID: failure.EventID, OperationID: failure.OperationID, OccurredAt: failure.OccurredAt,
		ActorID: failure.Actor.AccountID, Source: AuditSourceLocal, Action: "console.local_password_reset",
		ResourceKind: "local_account", ResourceID: failure.TargetID, Decision: "allowed", Outcome: AuditFailed,
	}
	if failure.Rejected {
		event.Outcome = AuditValidationRejected
	}
	if err := validateAuditEvent(event); err != nil || failure.OperationID == "" || failure.EventID == failure.OperationID {
		return ErrInvalidAuditEvent
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failed password reset: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back failed password reset: %w", err))
		}
	}()
	failure.OccurredAt, err = d.transactionTime(failure.OccurredAt)
	if err != nil {
		return err
	}
	event.OccurredAt = failure.OccurredAt
	_, consumeErr := consumeLocalPasswordReset(ctx, tx, failure.Actor, failure.TargetID, failure.OperationID, failure.OccurredAt)
	if errors.Is(consumeErr, identity.ErrPasswordResetExpired) {
		event.Decision, event.Outcome = "denied", AuditDenied
	} else if consumeErr != nil {
		return consumeErr
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed password reset: %w", err)
	}
	return consumeErr
}
