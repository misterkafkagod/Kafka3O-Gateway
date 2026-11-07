package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ReserveCognitoCodeExchange(ctx context.Context, request cognito.CodeExchangeReservation) (reservedAt time.Time, result error) {
	event := AuditEvent{
		EventID: request.OperationID + ".exchange", OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.exchange",
		Decision: "allowed", Outcome: AuditAttempted,
	}
	if request.OperationID == "" || request.DenialID == "" || request.DenialID == event.EventID ||
		request.ExpiresAt.IsZero() || request.ExpiresAt.Year() > 2261 {
		return time.Time{}, ErrInvalidAuditEvent
	}
	if !request.Attempt.MatchesCodeExchangeReservation(request.OperationID, request.ExpiresAt, request.OccurredAt, request.BrowserBindingHash) {
		return time.Time{}, cognito.ErrCodeExchangeUnavailable
	}
	if err := validateAuditEvent(event); err != nil {
		return time.Time{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("begin Cognito code exchange reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back Cognito code exchange reservation: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if request.BrowserBindingHash != ([32]byte{}) {
		transactionTime = d.strictTransactionTime
	}
	event.OccurredAt, err = transactionTime(request.OccurredAt)
	if err != nil {
		return time.Time{}, err
	}
	var accepted, reserved bool
	if err := tx.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ? AND operation_id = ? AND action = 'console.cognito.callback' AND outcome = 'accepted'),
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ?)`, request.OperationID+".callback", request.OperationID, event.EventID).Scan(&accepted, &reserved); err != nil {
		return time.Time{}, fmt.Errorf("inspect Cognito code exchange reservation: %w", err)
	}
	var denied error
	if !accepted || reserved || !request.ExpiresAt.After(event.OccurredAt) {
		event.EventID, event.Decision, event.Outcome = request.DenialID, "denied", AuditDenied
		denied = cognito.ErrCodeExchangeUnavailable
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return time.Time{}, errors.Join(cognito.ErrCodeExchangeAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, errors.Join(cognito.ErrCodeExchangeUnknown, fmt.Errorf("commit Cognito code exchange reservation: %w", err))
	}
	return event.OccurredAt, denied
}
