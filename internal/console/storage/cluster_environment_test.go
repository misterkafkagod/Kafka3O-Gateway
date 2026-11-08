package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestClusterEnvironmentBlocksWritesUnlessClassifiedAndActive(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	registerTestClusters(t, db, "dev", "prod", "retired", "unclassified")
	if _, err := db.db.ExecContext(ctx, `UPDATE registered_clusters SET environment = 'NON_PRODUCTION' WHERE id IN ('dev', 'retired');
UPDATE registered_clusters SET environment = 'PRODUCTION' WHERE id = 'prod'`); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{
		{ID: "dev", Name: "dev", GatewayURL: "https://dev.example"}, {ID: "prod", Name: "prod", GatewayURL: "https://prod.example"},
		{ID: "unclassified", Name: "unclassified", GatewayURL: "https://unclassified.example"},
	}, "deactivate-retired", time.Now()); err != nil {
		t.Fatal(err)
	}
	operator := []authorization.Grant{{Role: authorization.RoleOperator}}
	for _, tt := range []struct {
		cluster     string
		environment authorization.Environment
		allowed     bool
	}{
		{"dev", authorization.EnvironmentNonProduction, true},
		{"prod", authorization.EnvironmentProduction, false},
		{"retired", authorization.EnvironmentUnclassified, false},
		{"unclassified", authorization.EnvironmentUnclassified, false},
		{"unknown", authorization.EnvironmentUnclassified, false},
	} {
		environment, err := db.ClusterEnvironment(ctx, tt.cluster)
		if err != nil || environment != tt.environment {
			t.Fatalf("%s environment = %q, %v", tt.cluster, environment, err)
		}
		decision, err := authorization.EvaluatePolicy(authorization.PolicyRequest{
			Grants: operator, ClusterID: tt.cluster, Environment: environment, Action: authorization.ActionProduceMessage,
		})
		if err != nil || decision.Allowed != tt.allowed {
			t.Fatalf("%s OPERATOR produce decision = %+v, %v", tt.cluster, decision, err)
		}
	}
	if _, err := db.db.ExecContext(ctx, "DROP TABLE registered_clusters"); err != nil {
		t.Fatal(err)
	}
	if environment, err := db.ClusterEnvironment(ctx, "dev"); err == nil || environment != authorization.EnvironmentUnclassified {
		t.Fatalf("storage failure returned %q, %v", environment, err)
	}
}
