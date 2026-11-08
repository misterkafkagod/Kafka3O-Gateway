package storage

import (
	"context"
	"database/sql"
	"fmt"
)

type auditExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertAuditEvent(ctx context.Context, execer auditExecer, event AuditEvent) error {
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	const statement = `
INSERT INTO audit_events (
    event_id, operation_id, occurred_at_ns, actor_id, source, cluster_id,
    environment, action, resource_kind, resource_id, decision, outcome, reason, policy_revision,
    before_state, after_state
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := execer.ExecContext(ctx, statement,
		event.EventID, event.OperationID, event.OccurredAt.UTC().UnixNano(), event.ActorID,
		event.Source, event.ClusterID, event.Environment, event.Action, event.ResourceKind,
		event.ResourceID, event.Decision, event.Outcome, event.Reason, event.PolicyRevision,
		event.BeforeState, event.AfterState,
	)
	if err != nil {
		return fmt.Errorf("append audit event: %w", err)
	}
	return nil
}
