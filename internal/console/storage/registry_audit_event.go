package storage

import (
	"fmt"
	"time"
)

// registryAuditEvent describes one Console-service change to the cluster registry.
func registryAuditEvent(operationID string, sequence int, at time.Time, action, clusterID, environment string) AuditEvent {
	return AuditEvent{
		EventID: fmt.Sprintf("%s-%d", operationID, sequence), OperationID: operationID, OccurredAt: at,
		ActorID: "console", Source: AuditSourceService, ClusterID: clusterID, Environment: environment,
		Action: action, ResourceKind: "registered_cluster", ResourceID: clusterID,
		Decision: "allowed", Outcome: AuditSucceeded,
	}
}
