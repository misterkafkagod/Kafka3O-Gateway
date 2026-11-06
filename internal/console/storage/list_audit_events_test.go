package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestListAuditEventsScopesAndPaginates(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := time.Date(2026, 11, 6, 16, 0, 0, 0, time.UTC)
	for _, event := range []AuditEvent{
		{EventID: "a1", OccurredAt: base, ActorID: "u1", Source: AuditSourceLocal, ClusterID: "a", Action: "messages.produce", Outcome: AuditAttempted},
		{EventID: "global", OccurredAt: base.Add(time.Second), ActorID: "u1", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded},
		{EventID: "b1", OccurredAt: base.Add(2 * time.Second), ActorID: "u2", Source: AuditSourceCognito, ClusterID: "b", Action: "messages.produce", Outcome: AuditDenied},
		{EventID: "a2", OccurredAt: base.Add(3 * time.Second), ActorID: "u1", Source: AuditSourceLocal, ClusterID: "a", Action: "messages.produce", Outcome: AuditAccepted},
	} {
		if err := db.AppendAuditEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	first, err := db.ListAuditEvents(ctx, AuditFilter{ClusterID: "a", Limit: 1})
	if err != nil || len(first) != 1 || first[0].EventID != "a1" {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	next, err := db.ListAuditEvents(ctx, AuditFilter{ClusterID: "a", AfterSequence: first[0].Sequence, Limit: 10})
	if err != nil || len(next) != 1 || next[0].EventID != "a2" {
		t.Fatalf("next page = %+v, %v", next, err)
	}
	filtered, err := db.ListAuditEvents(ctx, AuditFilter{
		Global: true, ActorID: "u1", Action: "messages.produce", Outcome: AuditAccepted,
		From: base.Add(time.Second), Until: base.Add(4 * time.Second), Limit: 10,
	})
	if err != nil || len(filtered) != 1 || filtered[0].EventID != "a2" {
		t.Fatalf("filtered events = %+v, %v", filtered, err)
	}
}

func TestListAuditEventsRejectsAmbiguousScopeAndBounds(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now()
	for _, filter := range []AuditFilter{
		{Limit: 10}, {Global: true, ClusterID: "a", Limit: 10}, {ClusterID: "a", Limit: 0},
		{Global: true, Limit: 1001}, {Global: true, Limit: 1, AfterSequence: -1},
		{Global: true, Limit: 1, From: now, Until: now},
		{Global: true, Limit: 1, From: time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if _, err := db.ListAuditEvents(ctx, filter); !errors.Is(err, ErrInvalidAuditFilter) {
			t.Errorf("filter %+v: got %v", filter, err)
		}
	}
}
