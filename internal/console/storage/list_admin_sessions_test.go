package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestAdminSessionListingPaginatesActiveSessionsAndAudits(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	alice, first := f.addAccount(t, "alice-id", "alice", "", "")
	second, err := f.manager.IssueLocal(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	_, expired := f.addAccount(t, "expired-id", "expired-user", "", "")
	_, revoked := f.addAccount(t, "revoked-id", "revoked-user", "", "")
	disabled, _ := f.addAccount(t, "disabled-id", "disabled-user", "", "")
	stale, _ := f.addAccount(t, "stale-id", "stale-user", "", "")
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns + 1 WHERE id = ?", expired.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Revoke(ctx, revoked.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", disabled.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET credential_version = 2 WHERE identity_id = ?", stale.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", alice.ID); err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	expectedIDs := []string{f.adminRun.Principal.SessionID, first.Principal.SessionID, second.Principal.SessionID}
	sort.Strings(expectedIDs)
	page1, err := list.Execute(ctx, actor, "", 2)
	if err != nil || page1.OperationID == "" || len(page1.Sessions) != 2 ||
		page1.Sessions[0].ID != expectedIDs[0] || page1.Sessions[1].ID != expectedIDs[1] ||
		page1.NextCursor != expectedIDs[1] {
		t.Fatalf("first session page was invalid: %v", err)
	}
	page2, err := list.Execute(ctx, actor, page1.NextCursor, 2)
	if err != nil || page2.OperationID == "" || page2.OperationID == page1.OperationID || len(page2.Sessions) != 1 ||
		page2.Sessions[0].ID != expectedIDs[2] || page2.NextCursor != "" {
		t.Fatalf("second session page was invalid: %v", err)
	}
	empty, err := list.Execute(ctx, actor, expectedIDs[2], 2)
	if err != nil || empty.Sessions == nil || len(empty.Sessions) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty session page was invalid: %v", err)
	}
	afterRevoked, err := list.Execute(ctx, actor, revoked.Principal.SessionID, 10)
	if err != nil {
		t.Fatalf("a cursor from a removed session stopped pagination: %v", err)
	}
	var expectedAfter []string
	for _, id := range expectedIDs {
		if id > revoked.Principal.SessionID {
			expectedAfter = append(expectedAfter, id)
		}
	}
	if len(afterRevoked.Sessions) != len(expectedAfter) {
		t.Fatal("session cursor did not continue by opaque ID")
	}
	for i := range expectedAfter {
		if afterRevoked.Sessions[i].ID != expectedAfter[i] {
			t.Fatal("session cursor returned out-of-order data")
		}
	}
	for _, item := range append(page1.Sessions, page2.Sessions...) {
		if item.Current != (item.ID == actor.SessionID) || !item.CreatedAt.Before(item.IdleExpiresAt) ||
			item.IdleExpiresAt.After(item.AbsoluteExpiresAt) {
			t.Fatal("session summary has invalid identity or expiration metadata")
		}
		if item.ID == actor.SessionID {
			if item.AccountID != f.admin.ID || item.Username != "admin" {
				t.Fatal("current session has the wrong account")
			}
		} else if item.AccountID != alice.ID || item.Username != "alice" {
			t.Fatal("target session has the wrong account")
		}
	}
	encoded, err := json.Marshal([]identity.AdminSessionListResult{page1, page2})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"TokenHash", "CSRFHash", "CredentialVersion", "PasswordHash",
		first.Token, first.CSRFToken, second.Token, second.CSRFToken, f.adminRun.Token, f.adminRun.CSRFToken} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("session listing exposed credentials or credential metadata")
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.list", Limit: 10})
	if err != nil || len(events) != 4 {
		t.Fatalf("session listing audit count=%d err=%v", len(events), err)
	}
	for i, operation := range []string{page1.OperationID, page2.OperationID, empty.OperationID, afterRevoked.OperationID} {
		if events[i].OperationID != operation || events[i].ActorID != f.admin.ID || events[i].Outcome != AuditSucceeded ||
			events[i].ResourceID != "" || events[i].ResourceKind != "" {
			t.Fatal("session listing audit was not correlated or contained a cursor")
		}
	}
	for _, secret := range []string{"alice", "revoked-user", page1.NextCursor, revoked.Principal.SessionID, first.Token, first.CSRFToken} {
		if strings.Contains(fmt.Sprintf("%+v", events), secret) {
			t.Fatal("session listing audit contained a username, cursor or credential")
		}
	}
}

func TestAdminSessionListingRequiresCurrentGlobalAdmin(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	cluster := identity.LocalAdminActor{AccountID: clusterAdmin.ID, SessionID: clusterSession.Principal.SessionID}
	for _, tt := range []struct {
		actor identity.LocalAdminActor
		want  error
	}{
		{cluster, identity.ErrGlobalAdminRequired},
		{identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: cluster.SessionID}, identity.ErrInvalidAdminSession},
	} {
		result, err := list.Execute(ctx, tt.actor, "", 10)
		if !errors.Is(err, tt.want) || result.OperationID == "" || result.Sessions != nil {
			t.Fatalf("unauthorized session listing returned data: %v", err)
		}
	}
	if _, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if result, err := list.Execute(ctx, admin, "", 10); !errors.Is(err, identity.ErrGlobalAdminRequired) || result.Sessions != nil {
		t.Fatalf("removed grant remained effective: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', 'ADMIN')", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if result, err := list.Execute(ctx, admin, "", 10); !errors.Is(err, identity.ErrAdminPasswordChange) || result.Sessions != nil {
		t.Fatalf("restricted administrator listed sessions: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.list", Limit: 10})
	if err != nil || len(events) != 4 {
		t.Fatalf("session listing denial audit count=%d err=%v", len(events), err)
	}
	for _, event := range events {
		if event.Outcome != AuditDenied || event.ResourceID != "" {
			t.Fatal("session listing denial was not safely audited")
		}
	}
	if events[0].ActorID != clusterAdmin.ID || events[1].ActorID != "anonymous" {
		t.Fatal("session listing denial used an unverified actor")
	}
}

func TestAdminSessionListingRejectsInvalidPagination(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	for _, tt := range []struct {
		after string
		limit int
		want  error
	}{
		{"", 0, identity.ErrInvalidAdminSessionList},
		{"", identity.MaxAdminSessionPageSize + 1, identity.ErrInvalidAdminSessionList},
		{"submitted-invalid-cursor", 10, identity.ErrInvalidAdminSessionCursor},
		{strings.Repeat("x", 129), 10, identity.ErrInvalidAdminSessionCursor},
		{" leading-space", 10, identity.ErrInvalidAdminSessionCursor},
	} {
		result, err := list.Execute(ctx, actor, tt.after, tt.limit)
		if !errors.Is(err, tt.want) || result.OperationID == "" || result.Sessions != nil {
			t.Fatalf("invalid session listing returned data: %v", err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.list", Limit: 10})
	if err != nil || len(events) != 5 {
		t.Fatalf("invalid session listing audit count=%d err=%v", len(events), err)
	}
	for _, event := range events {
		if event.Outcome != AuditValidationRejected || event.ResourceID != "" || event.ActorID != f.admin.ID {
			t.Fatal("invalid session listing was not safely audited")
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", events), "submitted-invalid-cursor") {
		t.Fatal("session listing audit contained an untrusted cursor")
	}
}

func TestAdminSessionListingRechecksQueuedExpiry(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	start := time.Now().UTC().Add(time.Second)
	var current atomic.Int64
	current.Store(start.UnixNano())
	f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
		start.Add(time.Minute).UnixNano(), actor.SessionID); err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	var result identity.AdminSessionListResult
	err = runQueuedDatabaseOperation(t, f.db, func() {
		current.Store(start.Add(time.Minute).UnixNano())
	}, func() error {
		var err error
		result, err = list.Execute(ctx, actor, "", 10)
		return err
	})
	if !errors.Is(err, identity.ErrInvalidAdminSession) || result.Sessions != nil {
		t.Fatalf("queued expired administrator listed sessions: %v", err)
	}
}

func TestAdminSessionListingFailsClosedOnAuditFailure(t *testing.T) {
	for _, outcome := range []string{"succeeded", "denied", "validation_rejected"} {
		t.Run(outcome, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_session_list_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.list' AND NEW.outcome = '`+outcome+`'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			after := ""
			if outcome == "denied" {
				actor.SessionID = "invalid"
			} else if outcome == "validation_rejected" {
				after = "invalid"
			}
			result, err := list.Execute(ctx, actor, after, 10)
			if !errors.Is(err, identity.ErrAdminSessionListAudit) || result.OperationID == "" || result.Sessions != nil {
				t.Fatalf("unaudited session listing returned data or a denial: %v", err)
			}
		})
	}
}

func TestAdminSessionListingAuditCommitFailureReturnsNoData(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, err := f.db.db.ExecContext(ctx, `CREATE TABLE session_list_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_session_list_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.session.list'
BEGIN INSERT INTO session_list_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := list.Execute(ctx, actor, "", 10)
	if !errors.Is(err, identity.ErrAdminSessionListAudit) || result.OperationID == "" || result.Sessions != nil {
		t.Fatalf("failed session audit commit returned data: %v", err)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("failed session list affected the administrator: %v", err)
	}
}

func TestAdminSessionListingRejectsInvalidStoredMetadata(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET id = 'invalid-session-id' WHERE id = ?", target.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := list.Execute(ctx, actor, "", identity.MaxAdminSessionPageSize)
	if err == nil || result.Sessions != nil {
		t.Fatal("invalid stored session metadata was returned")
	}
}
