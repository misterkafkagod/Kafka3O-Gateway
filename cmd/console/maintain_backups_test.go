package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestBackupsAreConsistentSnapshotsWithRetention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	directory := t.TempDir()
	db, err := storage.OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.AppendAuditEvent(ctx, storage.AuditEvent{EventID: "e1", OperationID: "e1", OccurredAt: time.Now(), ActorID: "admin",
		Source: storage.AuditSourceLocal, Action: "login", Outcome: storage.AuditSucceeded}); err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Add(int64(time.Hour))) }
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintainBackups(ctx, db.Backup, directory, now, ticks, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	for i := 0; i < backupRetention+2; i++ {
		ticks <- time.Now()
	}
	cancel()
	<-done
	entries, _ := os.ReadDir(directory)
	if len(entries) != backupRetention {
		t.Fatalf("kept %d snapshots, want %d", len(entries), backupRetention)
	}
	info, _ := entries[0].Info()
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("snapshot mode = %v", info.Mode().Perm())
	}
	restored, err := storage.OpenDatabase(context.Background(), filepath.Join(directory, entries[len(entries)-1].Name()))
	if err != nil {
		t.Fatalf("snapshot did not reopen: %v", err)
	}
	defer restored.Close()
	events, err := restored.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 10})
	if err != nil || len(events) != 1 || events[0].EventID != "e1" {
		t.Fatalf("restored audit = %+v, %v", events, err)
	}
}
