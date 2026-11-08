package storage

import (
	"testing"
	"time"
)

func TestRegistryAuditEvent(t *testing.T) {
	at := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	event := registryAuditEvent("operation", 3, at, "console.cluster.deactivate", "prod", "PRODUCTION")
	want := AuditEvent{EventID: "operation-3", OperationID: "operation", OccurredAt: at, ActorID: "console",
		Source: AuditSourceService, ClusterID: "prod", Environment: "PRODUCTION", Action: "console.cluster.deactivate",
		ResourceKind: "registered_cluster", ResourceID: "prod", Decision: "allowed", Outcome: AuditSucceeded}
	if event != want {
		t.Fatalf("event = %+v, want %+v", event, want)
	}
	if err := validateAuditEvent(event); err != nil {
		t.Fatal(err)
	}
}
