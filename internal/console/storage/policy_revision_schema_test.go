package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// dropSchemaAfterVersionTen undoes schemas v11–v12 so older-version migration tests start from their real schema.
const dropSchemaAfterVersionTen = "DROP TABLE registered_clusters; DROP TABLE policy_state; ALTER TABLE audit_events DROP COLUMN before_state; ALTER TABLE audit_events DROP COLUMN after_state; "

func TestPolicyRevisionMigrationFromVersionTenPreservesAudit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := AuditEvent{EventID: "legacy", OperationID: "legacy", OccurredAt: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC),
		ActorID: "actor", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded}
	if err := db.AppendAuditEvent(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, dropSchemaAfterVersionTen+"PRAGMA user_version = 10"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		db, err = OpenDatabase(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		var version int
		var revision int64
		if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 12 {
			t.Fatalf("version-ten migration did not advance: version=%d err=%v", version, err)
		}
		if err := db.db.QueryRowContext(ctx, "SELECT revision FROM policy_state WHERE id = 1").Scan(&revision); err != nil || revision != 0 {
			t.Fatalf("initial policy revision = %d, %v", revision, err)
		}
		events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
		legacy.Sequence = 1
		if err != nil || len(events) != 1 || events[0] != legacy {
			t.Fatalf("migrated audit = %+v, %v", events, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolicyRevisionIncrementsOnlyOnCommit(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	advance := func(commit bool) int64 {
		t.Helper()
		tx, err := db.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Fatal(err)
			}
		}()
		revision, err := nextPolicyRevision(ctx, tx)
		if err != nil {
			t.Fatal(err)
		}
		if commit {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		return revision
	}
	if got := advance(true); got != 1 {
		t.Fatalf("first revision = %d, want 1", got)
	}
	if got := advance(false); got != 2 {
		t.Fatalf("uncommitted revision = %d, want 2", got)
	}
	if got := advance(true); got != 2 {
		t.Fatalf("revision after rollback = %d, want 2", got)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE policy_state SET revision = -1"); err == nil {
		t.Fatal("negative policy revision was accepted")
	}
	if _, err := db.db.ExecContext(ctx, "INSERT INTO policy_state (id, revision) VALUES (2, 0)"); err == nil {
		t.Fatal("a second policy revision row was accepted")
	}
}

func TestAuditEventBeforeAfterStatesRoundTripAndRejectFreeText(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	event := AuditEvent{EventID: "grant", OperationID: "grant", OccurredAt: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC),
		ActorID: "admin", Source: AuditSourceLocal, Action: "console.role_grant.change", ResourceKind: "identity",
		ResourceID: "alice", ClusterID: "cluster-a", Decision: "allowed", Outcome: AuditSucceeded, PolicyRevision: 1,
		BeforeState: "VIEWER", AfterState: "ADMIN"}
	if err := db.AppendAuditEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{ClusterID: "cluster-a", Limit: 10})
	event.Sequence = 1
	if err != nil || len(events) != 1 || events[0] != event {
		t.Fatalf("events = %+v, %v", events, err)
	}
	for _, state := range []string{"viewer", "secret-password", "ADMIN ", "UNCLASSIFIED"} {
		before, after := event, event
		before.Sequence, after.Sequence = 0, 0
		before.EventID, after.EventID = "before-"+state, "after-"+state
		before.BeforeState, after.AfterState = state, state
		if err := db.AppendAuditEvent(ctx, before); !errors.Is(err, ErrInvalidAuditEvent) {
			t.Fatalf("before state %q accepted: %v", state, err)
		}
		if err := db.AppendAuditEvent(ctx, after); !errors.Is(err, ErrInvalidAuditEvent) {
			t.Fatalf("after state %q accepted: %v", state, err)
		}
	}
}
