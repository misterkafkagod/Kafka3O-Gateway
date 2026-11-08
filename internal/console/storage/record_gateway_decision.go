package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// RecordGatewayDecision audits one delegated authorization decision; the change reason is recorded, never request data.
func (d *Database) RecordGatewayDecision(ctx context.Context, record delegation.DecisionRecord) error {
	source := AuditSource(record.Source)
	if source != AuditSourceLocal && source != AuditSourceCognito {
		source = AuditSourceAnonymous
	}
	actor := record.ActorID
	if actor == "" {
		actor = "anonymous"
	}
	event := AuditEvent{
		EventID: record.OperationID, OperationID: record.OperationID, OccurredAt: record.OccurredAt,
		ActorID: actor, Source: source, ClusterID: record.ClusterID, Environment: string(record.Environment),
		Action: string(record.Action), ResourceKind: "gateway_request", ResourceID: record.Resource,
		Decision: "denied", Outcome: AuditDenied, Reason: record.Reason,
	}
	if record.Allowed {
		event.Decision, event.Outcome = "allowed", AuditAttempted
	} else if record.Denial != "" {
		event.Decision = "denied:" + record.Denial
	}
	return d.AppendAuditEvent(ctx, event)
}
