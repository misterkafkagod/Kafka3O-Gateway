package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendAuditEventSurvivesReopenAndRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	event := AuditEvent{
		EventID: "event-1", OperationID: "operation-1", OccurredAt: time.Date(2026, 11, 6, 16, 0, 0, 123, time.UTC),
		ActorID: "local-user-1", Source: AuditSourceLocal, ClusterID: "cluster-a", Environment: "PRODUCTION",
		Action: "topics.config.alter", ResourceKind: "topic", ResourceID: "orders",
		Decision: "allowed", Outcome: AuditAttempted, Reason: "approved maintenance", PolicyRevision: 7,
	}
	if err := db.AppendAuditEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendAuditEvent(ctx, event); err == nil {
		t.Fatal("duplicate event ID was accepted")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.ListAuditEvents(ctx, AuditFilter{ClusterID: "cluster-a", Limit: 10})
	if err != nil || len(got) != 1 {
		t.Fatalf("events = %+v, %v", got, err)
	}
	event.Sequence = 1
	if got[0] != event {
		t.Fatalf("event = %+v, want %+v", got[0], event)
	}
}

func TestAppendAuditEventRejectsInvalidMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	valid := AuditEvent{EventID: "event", OccurredAt: time.Now().UTC(), ActorID: "actor", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded}
	cases := []AuditEvent{
		{}, {EventID: "event", OccurredAt: valid.OccurredAt, ActorID: "actor", Source: "other", Action: "login", Outcome: AuditSucceeded},
		{EventID: "event", OccurredAt: valid.OccurredAt, ActorID: "actor", Source: AuditSourceLocal, Action: "login", Outcome: "other"},
		{EventID: "event", OccurredAt: valid.OccurredAt, ActorID: "actor\nspoofed", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded},
		{EventID: "event", OccurredAt: valid.OccurredAt, ActorID: "actor", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded, Reason: string([]byte{0xff})},
	}
	for _, event := range cases {
		if err := db.AppendAuditEvent(ctx, event); !errors.Is(err, ErrInvalidAuditEvent) {
			t.Errorf("event %+v: got %v", event, err)
		}
	}
	got, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
	if err != nil || len(got) != 0 {
		t.Fatalf("invalid events were stored: %+v, %v", got, err)
	}
}
