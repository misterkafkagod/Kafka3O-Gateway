package storage

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestAdminSessionRevocationInvalidatesOnlyTargetAndAudits(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, first := f.addAccount(t, "subject-1", "other-user", "", "")
	second, err := f.manager.IssueLocal(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := revoke.Execute(ctx, actor, first.Principal.SessionID)
	if err != nil || result.OperationID == "" {
		t.Fatalf("revoke target session: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, first.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("revoked session remained usable: %v", err)
	}
	for _, token := range []string{second.Token, f.adminRun.Token} {
		if _, err := f.manager.Resolve(ctx, token); err != nil {
			t.Fatalf("unrelated session was revoked: %v", err)
		}
	}
	if _, err := revoke.Execute(ctx, actor, first.Principal.SessionID); !errors.Is(err, identity.ErrAdminSessionUnavailable) {
		t.Fatalf("already revoked session was accepted: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 10})
	if err != nil || len(events) != 4 || events[0].Outcome != AuditAttempted ||
		events[1].Outcome != AuditSucceeded || events[1].OperationID != result.OperationID ||
		events[1].ActorID != f.admin.ID || events[1].ResourceKind != "session" ||
		events[1].ResourceID != first.Principal.SessionID || events[3].Outcome != AuditValidationRejected ||
		events[3].ResourceID != "" {
		t.Fatalf("session revocation audit count=%d err=%v", len(events), err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{first.Token, first.CSRFToken, second.Token, second.CSRFToken,
		f.adminRun.Token, f.adminRun.CSRFToken, target.Username, "other strong passphrase"} {
		if strings.Contains(serialized, secret) {
			t.Fatal("session revocation audit exposed credentials or a username")
		}
	}
}

func TestAdminSessionSelfRevocationLeavesAccountAndOtherSessionUsable(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	other, err := f.manager.IssueLocal(ctx, f.admin)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := revoke.Execute(ctx, actor, actor.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("self-revoked session remained usable: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, other.Token); err != nil {
		t.Fatalf("self-revocation disabled another session: %v", err)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("self-revocation removed the administrator: %v", err)
	}
}

func TestAdminSessionRevocationRechecksAuthorization(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
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
		result, err := revoke.Execute(ctx, tt.actor, target.Principal.SessionID)
		if !errors.Is(err, tt.want) || result.OperationID == "" {
			t.Fatalf("unauthorized session revocation: %v", err)
		}
	}
	if _, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := revoke.Execute(ctx, admin, target.Principal.SessionID); !errors.Is(err, identity.ErrGlobalAdminRequired) {
		t.Fatalf("removed grant remained effective: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', 'ADMIN')", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := revoke.Execute(ctx, admin, target.Principal.SessionID); !errors.Is(err, identity.ErrAdminPasswordChange) {
		t.Fatalf("restricted administrator revoked a session: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 0 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Revoke(ctx, f.adminRun.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := revoke.Execute(ctx, admin, target.Principal.SessionID); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("revoked administrator revoked another session: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, target.Token); err != nil {
		t.Fatalf("denied administrator revoked target: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 20})
	if err != nil || len(events) != 10 {
		t.Fatalf("denial audit count=%d err=%v", len(events), err)
	}
	for i := 1; i < len(events); i += 2 {
		if events[i].Outcome != AuditDenied || events[i].ResourceID != "" {
			t.Fatal("denied revocation was not safely audited")
		}
	}
}

func TestAdminSessionRevocationRejectsInvalidAndUnavailableTargets(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, expired := f.addAccount(t, "expired-id", "expired-user", "", "")
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns + 1 WHERE id = ?", expired.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	unknown := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	for _, tt := range []struct {
		target string
		want   error
	}{
		{"untrusted-invalid-session", identity.ErrInvalidSessionID},
		{unknown, identity.ErrAdminSessionUnavailable},
		{expired.Principal.SessionID, identity.ErrAdminSessionUnavailable},
	} {
		result, err := revoke.Execute(ctx, actor, tt.target)
		if !errors.Is(err, tt.want) || result.OperationID == "" {
			t.Fatalf("invalid or unavailable session accepted: %v", err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 10})
	if err != nil || len(events) != 6 {
		t.Fatalf("invalid target audit count=%d err=%v", len(events), err)
	}
	for i := 1; i < len(events); i += 2 {
		if events[i].Outcome != AuditValidationRejected || events[i].ResourceID != "" {
			t.Fatal("invalid target was not safely audited")
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", events), "untrusted-invalid-session") ||
		strings.Contains(fmt.Sprintf("%+v", events), unknown) {
		t.Fatal("invalid or unknown target was recorded in audit")
	}
}

func TestAdminSessionRevocationRechecksQueuedExpiry(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	start := time.Now().UTC().Add(time.Second)
	var current atomic.Int64
	current.Store(start.UnixNano())
	f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
		start.Add(time.Minute).UnixNano(), actor.SessionID); err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	err = runQueuedDatabaseOperation(t, f.db, func() {
		current.Store(start.Add(time.Minute).UnixNano())
	}, func() error {
		_, err := revoke.Execute(ctx, actor, target.Principal.SessionID)
		return err
	})
	if !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("queued expired administrator revoked a session: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, target.Token); err != nil {
		t.Fatalf("queued denial revoked target: %v", err)
	}
}

func TestAdminSessionRevocationRollsBackWhenAuditFails(t *testing.T) {
	for _, outcome := range []string{"attempted", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			_, target := f.addAccount(t, "subject-1", "other-user", "", "")
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_session_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.admin_revoke' AND NEW.outcome = '`+outcome+`'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			result, err := revoke.Execute(ctx, actor, target.Principal.SessionID)
			if !errors.Is(err, identity.ErrAdminSessionRevokeAudit) || result.OperationID == "" {
				t.Fatalf("audit failure was not surfaced: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, target.Token); err != nil {
				t.Fatalf("session revocation was not rolled back: %v", err)
			}
		})
	}
}

func TestAdminSessionRevocationCommitFailureReportsUnknown(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	_, err := f.db.db.ExecContext(ctx, `CREATE TABLE session_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_session_commit AFTER UPDATE OF revoked_at_ns ON console_sessions
BEGIN INSERT INTO session_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := revoke.Execute(ctx, actor, target.Principal.SessionID)
	if !errors.Is(err, identity.ErrAdminSessionRevokeUnknown) || result.OperationID == "" {
		t.Fatalf("commit failure was not reported as unknown: %v", err)
	}
	path := f.db.path
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, target.Token); err != nil {
		t.Fatalf("failed commit left a partial revocation: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("failed commit audit count=%d err=%v", len(events), err)
	}
}

func TestAdminSessionRevocationSerializesConcurrentRequests(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	results := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := revoke.Execute(ctx, actor, target.Principal.SessionID)
			results <- err
		}()
	}
	group.Wait()
	close(results)
	var success, unavailable int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, identity.ErrAdminSessionUnavailable):
			unavailable++
		default:
			t.Errorf("unexpected concurrent revocation: %v", err)
		}
	}
	if success != 1 || unavailable != 1 {
		t.Fatalf("concurrent revocations: successes=%d unavailable=%d", success, unavailable)
	}
}
