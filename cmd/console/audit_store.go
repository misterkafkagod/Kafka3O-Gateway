package main

import (
	"context"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

// auditStore adapts the Console store to the HTTP audit interface (composition-root wiring).
type auditStore struct{ db *storage.Database }

func (s auditStore) ListAudit(ctx context.Context, filter consoleapi.AuditFilter) ([]consoleapi.AuditRecord, error) {
	events, err := s.db.ListAuditEvents(ctx, storage.AuditFilter{Global: filter.Global, ClusterID: filter.ClusterID, ActorID: filter.ActorID,
		Action: filter.Action, Outcome: storage.AuditOutcome(filter.Outcome), From: filter.From, Until: filter.Until,
		AfterSequence: filter.AfterSequence, Limit: filter.Limit})
	if err != nil {
		return nil, err
	}
	records := make([]consoleapi.AuditRecord, 0, len(events))
	for _, e := range events {
		records = append(records, consoleapi.AuditRecord{Sequence: e.Sequence, EventID: e.EventID, OperationID: e.OperationID, OccurredAt: e.OccurredAt,
			ActorID: e.ActorID, Source: string(e.Source), ClusterID: e.ClusterID, Environment: e.Environment, Action: e.Action,
			ResourceKind: e.ResourceKind, ResourceID: e.ResourceID, Decision: e.Decision, Outcome: string(e.Outcome), Reason: e.Reason,
			BeforeState: e.BeforeState, AfterState: e.AfterState, PolicyRevision: e.PolicyRevision})
	}
	return records, nil
}

func (s auditStore) AppendAudit(ctx context.Context, r consoleapi.AuditRecord) error {
	return s.db.AppendAuditEvent(ctx, storage.AuditEvent{EventID: r.EventID, OperationID: r.OperationID, OccurredAt: r.OccurredAt,
		ActorID: r.ActorID, Source: storage.AuditSource(r.Source), ClusterID: r.ClusterID, Environment: r.Environment, Action: r.Action,
		ResourceKind: r.ResourceKind, ResourceID: r.ResourceID, Decision: r.Decision, Outcome: storage.AuditOutcome(r.Outcome),
		Reason: r.Reason, BeforeState: r.BeforeState, AfterState: r.AfterState, PolicyRevision: r.PolicyRevision})
}
