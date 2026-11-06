package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ReserveCognitoCodeExchange(ctx context.Context, request cognito.CodeExchangeReservation) (result error) {
	event := AuditEvent{
		EventID: request.OperationID + ".exchange", OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.exchange",
		Decision: "allowed", Outcome: AuditAttempted,
	}
	if request.OperationID == "" || request.DenialID == "" || request.DenialID == event.EventID ||
		request.ExpiresAt.IsZero() || request.ExpiresAt.Year() > 2261 {
		return ErrInvalidAuditEvent
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Cognito code exchange reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back Cognito code exchange reservation: %w", err))
		}
	}()
	event.OccurredAt, err = d.transactionTime(request.OccurredAt)
	if err != nil {
		return err
	}
	var accepted, reserved bool
	if err := tx.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ? AND operation_id = ? AND action = 'console.cognito.callback' AND outcome = 'accepted'),
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ?)`, request.OperationID+".callback", request.OperationID, event.EventID).Scan(&accepted, &reserved); err != nil {
		return fmt.Errorf("inspect Cognito code exchange reservation: %w", err)
	}
	var denied error
	if !accepted || reserved || !request.ExpiresAt.After(event.OccurredAt) {
		event.EventID, event.Decision, event.Outcome = request.DenialID, "denied", AuditDenied
		denied = cognito.ErrCodeExchangeUnavailable
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(cognito.ErrCodeExchangeAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cognito.ErrCodeExchangeUnknown, fmt.Errorf("commit Cognito code exchange reservation: %w", err))
	}
	return denied
}
