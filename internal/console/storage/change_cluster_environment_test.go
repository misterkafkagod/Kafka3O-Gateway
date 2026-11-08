package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func classificationFixture(t *testing.T) (accountAdministrationFixture, identity.ChangeClusterEnvironment, identity.LocalAdminActor) {
	t.Helper()
	f := newAccountAdministrationFixture(t)
	if err := f.db.SyncRegisteredClusters(context.Background(), []identity.RegisteredCluster{
		{ID: "dev", Name: "Dev", GatewayURL: "https://dev.example"}, {ID: "prod", Name: "Prod", GatewayURL: "https://prod.example"},
	}, "sync", time.Now()); err != nil {
		t.Fatal(err)
	}
	service, err := identity.NewChangeClusterEnvironment(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return f, service, identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
}

func TestClusterEnvironmentClassificationIsAuditedWithReason(t *testing.T) {
	f, classify, admin := classificationFixture(t)
	ctx := context.Background()
	result, err := classify.Execute(ctx, admin, "prod", authorization.EnvironmentUnclassified, authorization.EnvironmentProduction, "Customer-facing cluster")
	if err != nil || result.PolicyRevision != 1 {
		t.Fatalf("classify = %+v, %v", result, err)
	}
	if got := registryRows(t, f.db)["prod"].environment; got != "PRODUCTION" {
		t.Fatalf("prod environment = %q", got)
	}
	if _, err := classify.Execute(ctx, admin, "prod", authorization.EnvironmentProduction, authorization.EnvironmentNonProduction, "Decommissioned"); err != nil {
		t.Fatal(err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{ClusterID: "prod", Action: "console.cluster.classify", Outcome: AuditSucceeded, Limit: 10})
	if err != nil || len(events) != 2 {
		t.Fatalf("classification audit = %+v, %v", events, err)
	}
	if e := events[0]; e.ActorID != f.admin.ID || e.BeforeState != "" || e.AfterState != "PRODUCTION" || e.Environment != "PRODUCTION" ||
		e.Reason != "Customer-facing cluster" || e.PolicyRevision != 1 || e.ResourceKind != "registered_cluster" {
		t.Fatalf("first classification audit = %+v", e)
	}
	if e := events[1]; e.BeforeState != "PRODUCTION" || e.AfterState != "NON_PRODUCTION" || e.PolicyRevision != 2 {
		t.Fatalf("second classification audit = %+v", e)
	}
}

func TestClusterEnvironmentClassificationRejectsUnauthorizedStaleAndUnknown(t *testing.T) {
	f, classify, admin := classificationFixture(t)
	ctx := context.Background()
	_, aliceRun := f.addAccount(t, "alice", "alice", "prod", "ADMIN")
	clusterAdmin := identity.LocalAdminActor{AccountID: "alice", SessionID: aliceRun.Principal.SessionID}
	if err := f.db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{{ID: "prod", Name: "Prod", GatewayURL: "https://prod.example"}}, "sync-2", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name        string
		actor       identity.LocalAdminActor
		cluster     string
		expected    authorization.Environment
		environment authorization.Environment
		reason      string
		want        error
	}{
		{"cluster admin", clusterAdmin, "prod", "", authorization.EnvironmentNonProduction, "downgrade", identity.ErrGlobalAdminRequired},
		{"stale expected", admin, "prod", authorization.EnvironmentProduction, authorization.EnvironmentNonProduction, "reason", identity.ErrPolicyConflict},
		{"inactive cluster", admin, "dev", "", authorization.EnvironmentProduction, "reason", identity.ErrUnknownCluster},
		{"unknown cluster", admin, "nope", "", authorization.EnvironmentProduction, "reason", identity.ErrUnknownCluster},
		{"missing reason", admin, "prod", "", authorization.EnvironmentProduction, "", identity.ErrChangeReasonRequired},
		{"padded reason", admin, "prod", "", authorization.EnvironmentProduction, " reason", identity.ErrChangeReasonRequired},
		{"unclassify", admin, "prod", "", authorization.EnvironmentUnclassified, "reason", identity.ErrInvalidAccessChange},
		{"unknown environment", admin, "prod", "", "STAGING", "reason", identity.ErrInvalidAccessChange},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := classify.Execute(ctx, tt.actor, tt.cluster, tt.expected, tt.environment, tt.reason); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
	for id, row := range registryRows(t, f.db) {
		if row.environment != "" {
			t.Fatalf("rejected classification changed %s to %q", id, row.environment)
		}
	}
	denied, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cluster.classify", Outcome: AuditDenied, Limit: 10})
	if err != nil || len(denied) != 1 || denied[0].ActorID != "alice" {
		t.Fatalf("cluster ADMIN denial audit = %+v, %v", denied, err)
	}
}

func TestClusterEnvironmentClassificationAuditFailureBlocksTheChange(t *testing.T) {
	f, classify, admin := classificationFixture(t)
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_classification_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cluster.classify' AND NEW.outcome = 'succeeded' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := classify.Execute(ctx, admin, "prod", "", authorization.EnvironmentProduction, "reason"); !errors.Is(err, identity.ErrAccessChangeAudit) {
		t.Fatalf("audit failure error = %v", err)
	}
	if got := registryRows(t, f.db)["prod"].environment; got != "" {
		t.Fatalf("unaudited classification was written: %q", got)
	}
}
