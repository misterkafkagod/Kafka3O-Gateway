package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) RecordCognitoRefreshFailure(ctx context.Context, failure cognito.RefreshFailure) (result error) {
	if identity.ValidateSessionID(failure.OperationID) != nil || identity.ValidateSessionID(failure.SessionID) != nil || failure.Generation < 1 {
		return cognito.ErrRefreshUnavailable
	}
	event := AuditEvent{
		EventID: failure.OperationID + ".refresh.outcome", OperationID: failure.OperationID,
		OccurredAt: failure.OccurredAt, ActorID: "anonymous", Source: AuditSourceAnonymous,
		Action: "console.cognito.refresh", ResourceKind: "session", ResourceID: failure.SessionID,
	}
	switch failure.Status {
	case cognito.RefreshFailedBeforeSend:
		event.Decision, event.Outcome = "denied", AuditFailedBeforeSend
	case cognito.RefreshOutcomeUnknown:
		event.Decision, event.Outcome = "unknown", AuditUnknown
	default:
		return cognito.ErrRefreshState
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Cognito refresh failure audit: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(cognito.ErrRefreshUnknown, result, fmt.Errorf("roll back Cognito refresh failure audit: %w", err))
		}
	}()
	event.OccurredAt, err = d.transactionTime(failure.OccurredAt)
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT actor_id FROM audit_events
WHERE event_id = ? AND operation_id = ? AND action = 'console.cognito.refresh'
AND outcome = 'attempted' AND source = 'cognito' AND resource_kind = 'session' AND resource_id = ?`,
		cognitoRefreshEventID(failure.SessionID, failure.Generation), failure.OperationID, failure.SessionID).Scan(&event.ActorID)
	if errors.Is(err, sql.ErrNoRows) {
		return cognito.ErrRefreshUnavailable
	}
	if err != nil {
		return fmt.Errorf("inspect Cognito refresh reservation for failure audit: %w", err)
	}
	event.Source = AuditSourceCognito
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(cognito.ErrRefreshAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh failure audit: %w", err))
	}
	return nil
}
