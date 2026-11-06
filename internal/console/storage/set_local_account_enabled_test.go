package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type accountAdministrationFixture struct {
	db       *Database
	manager  *session.Manager
	change   identity.SetLocalAccountEnabled
	admin    identity.LocalAccount
	adminRun session.Credentials
}

func newAccountAdministrationFixture(t *testing.T) accountAdministrationFixture {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	passwordChange, err := identity.NewChangeLocalPassword(authenticator, db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := passwordChange.Execute(ctx, admin.ID, admin.Username, "initial passphrase", "replacement passphrase", netip.MustParseAddr("192.0.2.1")); err != nil {
		t.Fatal(err)
	}
	admin, err = authenticator.Execute(ctx, admin.Username, "replacement passphrase", netip.MustParseAddr("192.0.2.1"))
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	adminRun, err := manager.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewSetLocalAccountEnabled(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return accountAdministrationFixture{db: db, manager: manager, change: change, admin: admin, adminRun: adminRun}
}

func (f accountAdministrationFixture) addAccount(t *testing.T, id, username, cluster, role string) (identity.LocalAccount, session.Credentials) {
	t.Helper()
	ctx := context.Background()
	hash, err := identity.HashLocalPassword(rand.Reader, "other strong passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES (?, 1)", id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `INSERT INTO local_accounts
(identity_id, username, password_hash, must_change_password, credential_version) VALUES (?, ?, ?, 0, 1)`, id, username, hash); err != nil {
		t.Fatal(err)
	}
	if role != "" {
		if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, ?, ?)", id, cluster, role); err != nil {
			t.Fatal(err)
		}
	}
	account := identity.LocalAccount{ID: id, Username: username, Enabled: true, CredentialVersion: 1}
	credentials, err := f.manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	return account, credentials
}

func TestAccountDisableAndEnableRevokeSessionsAndAudit(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	account, credentials := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.change.Execute(ctx, actor, account.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("disabled account session remained active: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, account.Username)
	if err != nil || stored.Enabled || stored.CredentialVersion != 2 {
		t.Fatalf("disabled account state: enabled=%t version=%d err=%v", stored.Enabled, stored.CredentialVersion, err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticator.Execute(ctx, account.Username, "other strong passphrase", netip.MustParseAddr("192.0.2.2")); !errors.Is(err, identity.ErrAuthenticationFailed) {
		t.Fatalf("disabled account authenticated: %v", err)
	}
	if _, err := f.change.Execute(ctx, actor, account.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("old session revived after re-enabling: %v", err)
	}
	stored, err = f.db.FindLocalAccount(ctx, account.Username)
	if err != nil || !stored.Enabled || stored.CredentialVersion != 3 {
		t.Fatalf("re-enabled account state: enabled=%t version=%d err=%v", stored.Enabled, stored.CredentialVersion, err)
	}
	if _, err := authenticator.Execute(ctx, account.Username, "other strong passphrase", netip.MustParseAddr("192.0.2.2")); err != nil {
		t.Fatalf("re-enabled account could not authenticate afresh: %v", err)
	}
	disable, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(disable) != 2 || disable[0].Outcome != AuditAttempted ||
		disable[1].Outcome != AuditSucceeded || disable[1].ActorID != f.admin.ID || disable[1].ResourceID != account.ID ||
		disable[1].OperationID != disable[0].EventID {
		t.Fatalf("disable audit count=%d err=%v", len(disable), err)
	}
	revocations, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.sessions.revoke_on_account_state_change", Limit: 10})
	if err != nil || len(revocations) != 1 || revocations[0].OperationID != disable[0].EventID {
		t.Fatalf("session revocation audit count=%d err=%v", len(revocations), err)
	}
	all, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"initial passphrase", "replacement passphrase", "other strong passphrase", "other-user", credentials.Token, credentials.CSRFToken} {
		if strings.Contains(fmt.Sprintf("%+v", all), value) {
			t.Fatal("account administration audit contains credentials or a username")
		}
	}
}

func TestReenablingAccountRevokesUnrevokedOldSessions(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	account, credentials := f.addAccount(t, "subject-1", "other-user", "", "")
	if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", account.ID); err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.change.Execute(ctx, actor, account.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("re-enabling revived a previously unrevoked session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.sessions.revoke_on_account_state_change", Limit: 10})
	if err != nil || len(events) != 1 || events[0].ResourceID != account.ID {
		t.Fatalf("re-enable revocation audit count=%d err=%v", len(events), err)
	}
}

func TestAccountStateChangeRechecksGlobalAdminAndSession(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: clusterAdmin.ID, SessionID: clusterSession.Principal.SessionID}
	if _, err := f.change.Execute(ctx, actor, target.ID, false); !errors.Is(err, identity.ErrGlobalAdminRequired) {
		t.Fatalf("cluster administrator changed a global account: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', 'ADMIN')", clusterAdmin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", clusterAdmin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.change.Execute(ctx, actor, target.ID, false); !errors.Is(err, identity.ErrGlobalAdminRequired) {
		t.Fatalf("stale global grant was accepted: %v", err)
	}
	if err := f.manager.Revoke(ctx, clusterSession.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := f.change.Execute(ctx, actor, target.ID, false); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("revoked administrator session was accepted: %v", err)
	}
	expiredManager, err := session.NewManager(f.db, rand.Reader, func() time.Time { return time.Now().Add(-time.Hour) }, time.Minute, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := expiredManager.IssueLocal(ctx, f.admin)
	if err != nil {
		t.Fatal(err)
	}
	expiredActor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: expired.Principal.SessionID}
	if _, err := f.change.Execute(ctx, expiredActor, target.ID, false); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("expired administrator session was accepted: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || !stored.Enabled {
		t.Fatalf("denied operations changed the target: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 20})
	if err != nil || len(events) != 8 || events[1].ActorID != clusterAdmin.ID ||
		events[3].ActorID != clusterAdmin.ID || events[5].ActorID != "anonymous" || events[5].Outcome != AuditDenied ||
		events[7].ActorID != "anonymous" || events[7].Outcome != AuditDenied {
		t.Fatalf("denied account administration audit count=%d err=%v", len(events), err)
	}
}

func TestTemporaryPasswordSessionCannotAdministerAccounts(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewSetLocalAccountEnabled(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: account.ID, SessionID: credentials.Principal.SessionID}
	if _, err := change.Execute(ctx, actor, account.ID, false); !errors.Is(err, identity.ErrAdminPasswordChange) {
		t.Fatalf("temporary-password session was allowed to administer accounts: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(events) != 2 || events[1].ActorID != account.ID || events[1].Outcome != AuditDenied {
		t.Fatalf("restricted administrator denial audit count=%d err=%v", len(events), err)
	}
}

func TestAccountStateChangeDoesNotAuditUntrustedTargetInput(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	const input = "not-a-stored-account"
	if _, err := f.change.Execute(ctx, actor, input, false); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("unknown account: %v", err)
	}
	if _, err := f.change.Execute(ctx, actor, "", false); !errors.Is(err, identity.ErrInvalidLocalAccount) {
		t.Fatalf("empty account ID: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(events) != 4 || events[1].Outcome != AuditValidationRejected ||
		events[3].Outcome != AuditValidationRejected || strings.Contains(fmt.Sprintf("%+v", events), input) {
		t.Fatalf("invalid-target audit count=%d err=%v", len(events), err)
	}
}

func TestAccountStateChangeProtectsLastLocalGlobalAdmin(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	firstActor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.change.Execute(ctx, firstActor, f.admin.ID, false); !errors.Is(err, identity.ErrLastLocalGlobalAdmin) {
		t.Fatalf("disabled the last local global administrator: %v", err)
	}
	second, secondSession := f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
	secondActor := identity.LocalAdminActor{AccountID: second.ID, SessionID: secondSession.Principal.SessionID}
	if _, err := f.change.Execute(ctx, secondActor, f.admin.ID, false); err != nil {
		t.Fatalf("could not disable an administrator while another remains: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("disabled administrator session remained active: %v", err)
	}
	if _, err := f.change.Execute(ctx, secondActor, second.ID, false); !errors.Is(err, identity.ErrLastLocalGlobalAdmin) {
		t.Fatalf("disabled the remaining administrator: %v", err)
	}
	if _, err := f.change.Execute(ctx, secondActor, f.admin.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("administrator session revived after re-enabling: %v", err)
	}
}

func TestConcurrentAdminDisablesCannotRemoveAllLocalGlobalAdmins(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	second, secondSession := f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
	actors := []identity.LocalAdminActor{
		{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID},
		{AccountID: second.ID, SessionID: secondSession.Principal.SessionID},
	}
	targets := []string{second.ID, f.admin.ID}
	results := make(chan error, 2)
	var group sync.WaitGroup
	for i := range actors {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			_, err := f.change.Execute(ctx, actors[i], targets[i], false)
			results <- err
		}(i)
	}
	group.Wait()
	close(results)
	var successes, denials int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, identity.ErrInvalidAdminSession), errors.Is(err, identity.ErrLastLocalGlobalAdmin):
			denials++
		default:
			t.Errorf("unexpected concurrent operation error: %v", err)
		}
	}
	if successes != 1 || denials != 1 {
		t.Fatalf("successes=%d denials=%d", successes, denials)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("concurrent changes removed every local administrator: %v", err)
	}
}

func TestAccountStateChangeRollsBackWhenAuditFails(t *testing.T) {
	for _, outcome := range []string{"attempted", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_account_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.disable' AND NEW.outcome = '`+outcome+`'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			if _, err := f.change.Execute(ctx, actor, target.ID, false); !errors.Is(err, identity.ErrAccountStateAudit) {
				t.Fatal("account state changed without durable audit")
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || !stored.Enabled || stored.CredentialVersion != 1 {
				t.Fatalf("account update was not rolled back: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
				t.Fatalf("session revocation was not rolled back: %v", err)
			}
		})
	}
}

func TestAccountStateCommitFailureReportsCorrelatedUnknownOutcome(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	_, err := f.db.db.ExecContext(ctx, `CREATE TABLE state_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_state_commit AFTER UPDATE OF enabled ON identities
WHEN NEW.id = 'subject-1'
BEGIN INSERT INTO state_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := f.change.Execute(ctx, actor, target.ID, false)
	if !errors.Is(err, identity.ErrAccountStateUnknown) || result.OperationID == "" {
		t.Fatalf("commit failure was not reported as unknown: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || !stored.Enabled || stored.CredentialVersion != target.CredentialVersion {
		t.Fatalf("failed commit exposed a partial account change before reopening: %v", err)
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
	stored, err = db.FindLocalAccount(ctx, target.Username)
	if err != nil || !stored.Enabled || stored.CredentialVersion != target.CredentialVersion {
		t.Fatalf("failed commit left a partial account change: %v", err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("failed commit left a partial session revocation: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("failed commit audit count=%d err=%v", len(events), err)
	}
}
