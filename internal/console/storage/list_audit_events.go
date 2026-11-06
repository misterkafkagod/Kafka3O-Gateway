package storage

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (d *Database) ListAuditEvents(ctx context.Context, filter AuditFilter) ([]AuditEvent, error) {
	if filter.Limit < 1 || filter.Limit > 1000 || filter.AfterSequence < 0 ||
		(filter.Global && filter.ClusterID != "") || (!filter.Global && strings.TrimSpace(filter.ClusterID) == "") ||
		(!filter.From.IsZero() && (filter.From.Year() < 1970 || filter.From.Year() > 2261)) ||
		(!filter.Until.IsZero() && (filter.Until.Year() < 1970 || filter.Until.Year() > 2261)) ||
		(!filter.From.IsZero() && !filter.Until.IsZero() && !filter.From.Before(filter.Until)) {
		return nil, ErrInvalidAuditFilter
	}
	query := `SELECT sequence, event_id, operation_id, occurred_at_ns, actor_id, source,
cluster_id, environment, action, resource_kind, resource_id, decision, outcome, reason, policy_revision
FROM audit_events WHERE sequence > ?`
	args := []any{filter.AfterSequence}
	if !filter.Global {
		query += " AND cluster_id = ?"
		args = append(args, filter.ClusterID)
	}
	if filter.ActorID != "" {
		query += " AND actor_id = ?"
		args = append(args, filter.ActorID)
	}
	if filter.Action != "" {
		query += " AND action = ?"
		args = append(args, filter.Action)
	}
	if filter.Outcome != "" {
		query += " AND outcome = ?"
		args = append(args, filter.Outcome)
	}
	if !filter.From.IsZero() {
		query += " AND occurred_at_ns >= ?"
		args = append(args, filter.From.UTC().UnixNano())
	}
	if !filter.Until.IsZero() {
		query += " AND occurred_at_ns < ?"
		args = append(args, filter.Until.UTC().UnixNano())
	}
	query += " ORDER BY sequence ASC LIMIT ?"
	args = append(args, filter.Limit)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()
	events := make([]AuditEvent, 0)
	for rows.Next() {
		var event AuditEvent
		var occurredAt int64
		if err := rows.Scan(
			&event.Sequence, &event.EventID, &event.OperationID, &occurredAt, &event.ActorID,
			&event.Source, &event.ClusterID, &event.Environment, &event.Action, &event.ResourceKind,
			&event.ResourceID, &event.Decision, &event.Outcome, &event.Reason, &event.PolicyRevision,
		); err != nil {
			return nil, fmt.Errorf("read audit event: %w", err)
		}
		event.OccurredAt = time.Unix(0, occurredAt).UTC()
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}
