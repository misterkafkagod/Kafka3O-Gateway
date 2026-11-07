package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) reserveCognitoSession(ctx context.Context, record session.CognitoRecord, details cognito.SessionExchangeDetails) (result error) {
	event := AuditEvent{
		EventID: details.OperationID + ".session.attempt", OperationID: details.OperationID, OccurredAt: record.Session.CreatedAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.session",
		Decision: "allowed", Outcome: AuditAttempted,
	}
	if record.Session.EventID == "" || record.Session.EventID == event.EventID {
		return ErrInvalidAuditEvent
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Cognito session reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(session.ErrCognitoSessionUnknown, result, fmt.Errorf("roll back Cognito session reservation: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if record.Session.BrowserBindingHash != ([32]byte{}) {
		transactionTime = d.strictTransactionTime
	}
	event.OccurredAt, err = transactionTime(record.Session.CreatedAt)
	if err != nil {
		return err
	}
	var exchangeSucceeded, reserved bool
	if err := tx.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ? AND operation_id = ?
  AND action = 'console.cognito.exchange' AND decision = 'allowed' AND outcome = 'succeeded'),
EXISTS(SELECT 1 FROM audit_events WHERE event_id = ?)`,
		details.OperationID+".exchange.outcome", details.OperationID, event.EventID).Scan(&exchangeSucceeded, &reserved); err != nil {
		return fmt.Errorf("inspect Cognito session reservation: %w", err)
	}
	currentDetails, proofErr := cognitoSessionExchangeDetails(record, event.OccurredAt)
	if proofErr == nil && currentDetails.OperationID != details.OperationID {
		proofErr = cognito.ErrCodeExchangeUnavailable
	}
	var denied error
	if !exchangeSucceeded || reserved || proofErr != nil || !record.Session.IdleExpiresAt.After(event.OccurredAt) {
		event.EventID, event.Decision, event.Outcome = record.Session.EventID, "denied", AuditDenied
		denied = errors.Join(session.ErrCognitoSessionUnavailable, proofErr)
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(session.ErrCognitoSessionAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(session.ErrCognitoSessionUnknown, fmt.Errorf("commit Cognito session reservation: %w", err))
	}
	return denied
}
