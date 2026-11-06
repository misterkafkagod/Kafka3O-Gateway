package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupCreatesConsistentSnapshotWithoutOverwriting(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := OpenDatabase(ctx, filepath.Join(dir, "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	event := AuditEvent{EventID: "first", OccurredAt: time.Now().UTC(), ActorID: "user", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded}
	if err := db.AppendAuditEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(dir, "backup.sqlite")
	if err := db.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	event.EventID = "later"
	if err := db.AppendAuditEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := OpenDatabase(ctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	got, err := snapshot.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
	if err != nil || len(got) != 1 || got[0].EventID != "first" {
		t.Fatalf("snapshot = %+v, %v", got, err)
	}
	if err := db.Backup(ctx, backup); err == nil {
		t.Fatal("existing backup was overwritten")
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("existing backup was removed: %v", err)
	}
}
