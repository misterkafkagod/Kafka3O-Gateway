package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestListActiveClustersOmitsInactiveClusters(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	registerTestClusters(t, db, "b", "a", "old")
	if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{
		{ID: "a", Name: "a", GatewayURL: "https://a.example"}, {ID: "b", Name: "b", GatewayURL: "https://b.example"},
	}, "deactivate-old", time.Now()); err != nil {
		t.Fatal(err)
	}
	clusters, err := db.ListActiveClusters(ctx)
	if err != nil || len(clusters) != 2 || clusters[0].ID != "a" || clusters[1].ID != "b" || clusters[0].GatewayURL != "https://a.example" || !clusters[0].Active {
		t.Fatalf("active clusters = %+v, %v", clusters, err)
	}
}
