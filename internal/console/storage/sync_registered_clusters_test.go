package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

type registryRow struct {
	name, url, environment string
	active                 bool
}

func registryRows(t *testing.T, db *Database) map[string]registryRow {
	t.Helper()
	rows, err := db.db.Query("SELECT id, display_name, gateway_url, environment, active FROM registered_clusters")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]registryRow{}
	for rows.Next() {
		var id string
		var row registryRow
		if err := rows.Scan(&id, &row.name, &row.url, &row.environment, &row.active); err != nil {
			t.Fatal(err)
		}
		result[id] = row
	}
	return result
}

func TestRegisteredClusterSyncRegistersRenamesDeactivatesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	clusters := []identity.RegisteredCluster{
		{ID: "dev", Name: "Dev", GatewayURL: "https://dev.example:8081"},
		{ID: "prod", Name: "Prod", GatewayURL: "https://prod.example:8082"},
	}
	if err := db.SyncRegisteredClusters(ctx, clusters, "sync-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE registered_clusters SET environment = 'PRODUCTION' WHERE id = 'prod'"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SyncRegisteredClusters(ctx, clusters, "sync-2", time.Now()); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil || len(events) != 2 || events[0].Action != "console.cluster.register" || events[0].Source != AuditSourceService {
		t.Fatalf("idempotent sync audit = %+v, %v", events, err)
	}
	renamed := []identity.RegisteredCluster{{ID: "prod", Name: "Production", GatewayURL: "https://prod.example:8082"}}
	if err := db.SyncRegisteredClusters(ctx, renamed, "sync-3", time.Now()); err != nil {
		t.Fatal(err)
	}
	want := map[string]registryRow{
		"dev":  {"Dev", "https://dev.example:8081", "", false},
		"prod": {"Production", "https://prod.example:8082", "PRODUCTION", true},
	}
	if got := registryRows(t, db); got["dev"] != want["dev"] || got["prod"] != want["prod"] {
		t.Fatalf("registry = %+v", got)
	}
	if err := db.SyncRegisteredClusters(ctx, clusters, "sync-4", time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := registryRows(t, db)["dev"]; !got.active || got.environment != "" {
		t.Fatalf("reactivated dev = %+v", got)
	}
	events, err = db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	var actions []string
	for _, event := range events {
		actions = append(actions, event.Action+":"+event.ResourceID)
	}
	wantActions := []string{"console.cluster.register:dev", "console.cluster.register:prod", "console.cluster.rename:prod",
		"console.cluster.deactivate:dev", "console.cluster.reactivate:dev", "console.cluster.rename:prod"}
	if err != nil || len(actions) != len(wantActions) {
		t.Fatalf("registry audit = %v, %v", actions, err)
	}
	for i := range wantActions {
		if actions[i] != wantActions[i] {
			t.Fatalf("registry audit = %v, want %v", actions, wantActions)
		}
	}
}

func TestRegisteredClusterSyncRefusesRetargetAndInvalidInput(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{{ID: "prod", Name: "Prod", GatewayURL: "https://prod.example"}}, "sync-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	retarget := []identity.RegisteredCluster{
		{ID: "other", Name: "Other", GatewayURL: "https://other.example"},
		{ID: "prod", Name: "Prod", GatewayURL: "https://attacker.example"},
	}
	if err := db.SyncRegisteredClusters(ctx, retarget, "sync-2", time.Now()); !errors.Is(err, identity.ErrClusterRetarget) {
		t.Fatalf("retarget error = %v", err)
	}
	if rows := registryRows(t, db); len(rows) != 1 || rows["prod"].url != "https://prod.example" {
		t.Fatalf("refused sync partially applied: %+v", rows)
	}
	for _, clusters := range [][]identity.RegisteredCluster{
		{{ID: "bad id", Name: "Bad", GatewayURL: "https://x.example"}},
		{{ID: "dup", Name: "A", GatewayURL: "https://a.example"}, {ID: "dup", Name: "B", GatewayURL: "https://b.example"}},
		{{ID: "noname", GatewayURL: "https://x.example"}},
	} {
		if err := db.SyncRegisteredClusters(ctx, clusters, "sync-3", time.Now()); !errors.Is(err, identity.ErrInvalidClusterRegistry) {
			t.Fatalf("invalid registry %+v error = %v", clusters, err)
		}
	}
}
