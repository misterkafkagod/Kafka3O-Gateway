package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// RecordGatewayOutcome appends the one outcome of an allowed mutation intent, correlated by operation ID and
// attributed to the intent's actor; it never changes the intent itself.
func (d *Database) RecordGatewayOutcome(ctx context.Context, record delegation.OutcomeRecord) (result error) {
	outcomes := map[delegation.Outcome]AuditOutcome{
		delegation.OutcomeConfirmed: AuditConfirmed, delegation.OutcomeValidationRejected: AuditValidationRejected,
		delegation.OutcomeFailed: AuditFailed, delegation.OutcomeUnknown: AuditUnknown,
	}
	outcome, ok := outcomes[record.Outcome]
	if !ok {
		return delegation.ErrInvalidOutcome
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin gateway outcome: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back gateway outcome: %w", err))
		}
	}()
	var intent AuditEvent
	err = tx.QueryRowContext(ctx, `SELECT actor_id, source, cluster_id, environment, action, resource_id, reason, policy_revision
FROM audit_events WHERE event_id = ? AND resource_kind = 'gateway_request' AND decision = 'allowed'`, record.OperationID).Scan(
		&intent.ActorID, &intent.Source, &intent.ClusterID, &intent.Environment, &intent.Action, &intent.ResourceID, &intent.Reason, &intent.PolicyRevision)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && intent.ClusterID != record.CallerClusterID) {
		return delegation.ErrUnknownIntent
	}
	if err != nil {
		return fmt.Errorf("read gateway intent: %w", err)
	}
	var recorded bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM audit_events WHERE operation_id = ? AND resource_kind = 'gateway_outcome')`,
		record.OperationID).Scan(&recorded); err != nil {
		return fmt.Errorf("inspect gateway outcome: %w", err)
	}
	if recorded {
		return delegation.ErrDuplicateOutcome
	}
	at, err := d.transactionTime(record.OccurredAt)
	if err != nil {
		return err
	}
	intent.EventID, intent.OperationID, intent.OccurredAt = record.OperationID+"-outcome", record.OperationID, at
	intent.ResourceKind, intent.Decision, intent.Outcome = "gateway_outcome", "allowed", outcome
	if err := insertAuditEvent(ctx, tx, intent); err != nil {
		return fmt.Errorf("record gateway outcome: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit gateway outcome: %w", err)
	}
	return nil
}
