package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) FinishCognitoCodeExchange(ctx context.Context, outcome cognito.CodeExchangeOutcome) (completedAt time.Time, result error) {
	event := AuditEvent{
		EventID: outcome.OperationID + ".exchange.outcome", OperationID: outcome.OperationID, OccurredAt: outcome.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.exchange",
		Decision: "denied", Outcome: AuditFailed,
	}
	if outcome.OperationID == "" {
		return time.Time{}, ErrInvalidAuditEvent
	}
	switch outcome.Status {
	case cognito.CodeExchangeSucceeded:
		if outcome.ValidUntil.IsZero() || outcome.ValidUntil.Year() > 2261 {
			return time.Time{}, ErrInvalidAuditEvent
		}
		event.Decision, event.Outcome = "allowed", AuditSucceeded
	case cognito.CodeExchangeFailed:
	case cognito.CodeExchangeUnknown:
		event.Decision, event.Outcome = "unknown", AuditUnknown
	default:
		return time.Time{}, ErrInvalidAuditEvent
	}
	if err := validateAuditEvent(event); err != nil {
		return time.Time{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("begin Cognito code exchange outcome: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back Cognito code exchange outcome: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if outcome.Bound {
		transactionTime = d.strictTransactionTime
	}
	event.OccurredAt, err = transactionTime(outcome.OccurredAt)
	if err != nil {
		return time.Time{}, err
	}
	var reserved bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM audit_events
WHERE event_id = ? AND operation_id = ? AND action = 'console.cognito.exchange' AND outcome = 'attempted')`,
		outcome.OperationID+".exchange", outcome.OperationID).Scan(&reserved); err != nil {
		return time.Time{}, fmt.Errorf("inspect Cognito code exchange outcome: %w", err)
	}
	if !reserved {
		return time.Time{}, cognito.ErrCodeExchangeUnavailable
	}
	var decision error
	if outcome.Status == cognito.CodeExchangeSucceeded && !outcome.ValidUntil.After(event.OccurredAt) {
		event.Decision, event.Outcome = "denied", AuditValidationRejected
		decision = cognito.ErrCodeExchangeExpired
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return time.Time{}, errors.Join(cognito.ErrCodeExchangeAudit, err)
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, errors.Join(cognito.ErrCodeExchangeUnknown, fmt.Errorf("commit Cognito code exchange outcome: %w", err))
	}
	return event.OccurredAt, decision
}
