package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) recordCognitoRefreshFailureWithBrowserBinding(ctx context.Context, failure cognito.RefreshFailure, browserHash [32]byte) (result error) {
	if ctx == nil || identity.ValidateSessionID(failure.OperationID) != nil || identity.ValidateSessionID(failure.SessionID) != nil || failure.Generation < 1 {
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
	transactionTime := d.transactionTime
	if browserHash != ([32]byte{}) {
		transactionTime = d.strictTransactionTime
	}
	event.OccurredAt, err = transactionTime(failure.OccurredAt)
	if err != nil {
		return err
	}
	bindingPredicate := "(console_sessions.id IS NULL OR console_sessions.browser_binding_hash IS NULL)"
	args := []any{cognitoRefreshEventID(failure.SessionID, failure.Generation), failure.OperationID, failure.SessionID}
	if browserHash != ([32]byte{}) {
		bindingPredicate = "console_sessions.browser_binding_hash = ? AND console_sessions.identity_id = audit_events.actor_id AND audit_events.decision = 'allowed'"
		args = append(args, browserHash[:])
	}
	var attemptedAt int64
	var createdAt sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT audit_events.actor_id, audit_events.occurred_at_ns, console_sessions.created_at_ns
FROM audit_events LEFT JOIN console_sessions ON console_sessions.id = audit_events.resource_id
WHERE audit_events.event_id = ? AND audit_events.operation_id = ? AND audit_events.action = 'console.cognito.refresh'
AND audit_events.outcome = 'attempted' AND audit_events.source = 'cognito'
AND audit_events.resource_kind = 'session' AND audit_events.resource_id = ? AND `+bindingPredicate,
		args...).Scan(&event.ActorID, &attemptedAt, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return cognito.ErrRefreshUnavailable
	}
	if err != nil {
		return fmt.Errorf("inspect Cognito refresh reservation for failure audit: %w", err)
	}
	if browserHash != ([32]byte{}) {
		if !createdAt.Valid || createdAt.Int64 > failure.OccurredAt.UnixNano() || attemptedAt > failure.OccurredAt.UnixNano() {
			return ErrInvalidAuditEvent
		}
		event.OccurredAt, err = transactionTime(event.OccurredAt)
		if err != nil {
			return err
		}
	}
	event.Source = AuditSourceCognito
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(cognito.ErrRefreshAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cognito.ErrRefreshUnknown, fmt.Errorf("commit Cognito refresh failure audit: %w", err))
	}
	if browserHash != ([32]byte{}) {
		if _, err := transactionTime(event.OccurredAt); err != nil {
			return errors.Join(cognito.ErrRefreshUnknown, err)
		}
	}
	return nil
}
