package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundAdminFixture struct {
	*cognitoCodeFixture
	binding [32]byte
	marker  string
	token   string
	record  session.Record
	actor   identity.LocalAdminActor
}

func newBoundAdminFixture(t *testing.T) *boundAdminFixture {
	t.Helper()
	fixture, binding, marker := newBrowserSessionFixture(t)
	f := &boundAdminFixture{cognitoCodeFixture: fixture, binding: binding, marker: marker}
	if _, err := f.db.db.Exec("INSERT INTO role_grants (account_id, cluster_id, role) VALUES ('browser-local-a', '', 'ADMIN')"); err != nil {
		t.Fatal(err)
	}
	f.record, f.token = browserSessionRecord("bound-admin", "browser-local-a", binding, f.clock())
	f.record.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{31}, 16))
	if _, err := f.db.CreateLocalSession(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	f.actor = identity.LocalAdminActor{AccountID: f.record.AccountID, SessionID: f.record.SessionID, BrowserBindingHash: binding}
	return f
}

func TestBoundAdminActionFamiliesRecheckBinding(t *testing.T) {
	f := newBoundAdminFixture(t)
	ctx := context.Background()
	target, _ := browserSessionRecord("bound-admin-target", "browser-local-b", [32]byte{}, f.clock())
	target.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{32}, 16))
	if _, err := f.db.CreateLocalSession(ctx, target); err != nil {
		t.Fatal(err)
	}
	accounts, err := identity.NewListLocalAccounts(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := identity.NewListAdminSessions(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	create, err := identity.NewCreateLocalAccount(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewSetLocalAccountEnabled(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "wrong"} {
		actor := f.actor
		actor.BrowserBindingHash = [32]byte{}
		if mode == "wrong" {
			actor.BrowserBindingHash = sha256.Sum256([]byte("wrong browser"))
		}
		for _, action := range []struct {
			name string
			run  func() error
		}{
			{"accounts", func() error { _, err := accounts.Execute(ctx, actor, "", 10); return err }},
			{"sessions", func() error { _, err := sessions.Execute(ctx, actor, "", 10); return err }},
			{"create", func() error { _, err := create.Execute(ctx, actor, "newuser", "temporary passphrase"); return err }},
			{"reset", func() error {
				_, err := reset.Execute(ctx, actor, "browser-local-b", "temporary passphrase")
				return err
			}},
			{"state", func() error { _, err := change.Execute(ctx, actor, "browser-local-b", false); return err }},
			{"revoke", func() error { _, err := revoke.Execute(ctx, actor, target.SessionID); return err }},
		} {
			if err := action.run(); !errors.Is(err, identity.ErrInvalidAdminSession) {
				t.Fatalf("%s actor with %s binding was not denied: %v", action.name, mode, err)
			}
		}
	}
	if page, err := accounts.Execute(ctx, f.actor, "", 10); err != nil || len(page.Accounts) != 2 {
		t.Fatalf("bound global admin could not list accounts: %v", err)
	}
	if page, err := sessions.Execute(ctx, f.actor, "", 10); err != nil || len(page.Sessions) != 2 {
		t.Fatalf("bound global admin could not list sessions: %v", err)
	}
	if _, err := revoke.Execute(ctx, f.actor, target.SessionID); err != nil {
		t.Fatalf("bound global admin could not revoke a target session: %v", err)
	}
	if _, err := reset.Execute(ctx, f.actor, "browser-local-b", "temporary passphrase"); err != nil {
		t.Fatalf("bound global admin could not reset a password: %v", err)
	}
	if _, err := change.Execute(ctx, f.actor, "browser-local-b", false); err != nil {
		t.Fatalf("bound global admin could not disable an account: %v", err)
	}
	if _, err := change.Execute(ctx, f.actor, "browser-local-b", true); err != nil {
		t.Fatalf("bound global admin could not enable an account: %v", err)
	}
	created, err := create.Execute(ctx, f.actor, "newuser", "temporary passphrase")
	if err != nil || created.Account.Username != "newuser" || !created.Account.MustChangePassword {
		t.Fatalf("bound global admin could not create an account: %v", err)
	}
	actorJSON, err := json.Marshal(f.actor)
	if err != nil || strings.Contains(string(actorJSON), "BrowserBindingHash") || strings.Contains(string(actorJSON), f.marker) {
		t.Fatal("administrator actor serialization exposed browser binding material")
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{f.marker, f.token, "temporary passphrase"} {
		if strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("administrator audit exposed browser or credential material")
		}
	}
}

func TestBoundAdminRejectsInactiveAndChangedAuthority(t *testing.T) {
	for _, mode := range []string{"revoked", "disabled", "expired", "credential-version", "grant-removed", "password-change", "mapping-removed"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundAdminFixture(t)
			ctx := context.Background()
			want := identity.ErrInvalidAdminSession
			switch mode {
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, f.record.TokenHash, f.binding, "bound-admin-revoked", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.record.IdleExpiresAt.UnixNano())
			case "credential-version":
				if _, err := f.db.db.Exec("UPDATE local_accounts SET credential_version = 2 WHERE identity_id = ?", f.record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "grant-removed":
				if _, err := f.db.db.Exec("UPDATE role_grants SET role = 'VIEWER' WHERE account_id = ? AND cluster_id = ''", f.record.AccountID); err != nil {
					t.Fatal(err)
				}
				want = identity.ErrGlobalAdminRequired
			case "password-change":
				if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.record.AccountID); err != nil {
					t.Fatal(err)
				}
				want = identity.ErrAdminPasswordChange
			case "mapping-removed":
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:]); err != nil {
					t.Fatal(err)
				}
			}
			page, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
				Actor: f.actor, Limit: 10, OperationID: "inactive-bound-admin-list", OccurredAt: f.clock(),
			})
			if !errors.Is(err, want) || len(page.Accounts) != 0 {
				t.Fatalf("inactive bound administrator received account data: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied {
				t.Fatal("inactive bound administrator denial was not audited")
			}
		})
	}
}

func TestBoundAdminCannotReviveAfterReplacement(t *testing.T) {
	f := newBoundAdminFixture(t)
	ctx := context.Background()
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.record.AccountID); err != nil {
		t.Fatal(err)
	}
	replacement, _ := browserSessionRecord("replacement-admin", "browser-local-b", f.binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.record.AccountID); err != nil {
		t.Fatal(err)
	}
	page, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
		Actor: f.actor, Limit: 10, OperationID: "replaced-bound-admin-list", OccurredAt: f.clock(),
	})
	if !errors.Is(err, identity.ErrInvalidAdminSession) || len(page.Accounts) != 0 {
		t.Fatal("replaced browser binding revived an old administrator")
	}
	if principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != replacement.SessionID {
		t.Fatal("old administrator reauthorization affected the replacement")
	}
}

func TestBoundAdminQueuedBehindBindingReplacement(t *testing.T) {
	f := newBoundAdminFixture(t)
	ctx := context.Background()
	other, _ := browserSessionRecord("queued-admin-other", "browser-local-b", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, other); err != nil {
		t.Fatal(err)
	}
	tx, err := f.db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE console_browser_sessions SET session_id = ? WHERE browser_hash = ?", other.SessionID, f.binding[:]); err != nil {
		t.Fatal(err)
	}
	waitCount := f.db.db.Stats().WaitCount
	result := make(chan error, 1)
	go func() {
		page, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
			Actor: f.actor, Limit: 10, OperationID: "queued-bound-admin-list", OccurredAt: f.clock(),
		})
		if len(page.Accounts) != 0 {
			result <- errors.New("queued stale administrator received account data")
			return
		}
		result <- err
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for f.db.db.Stats().WaitCount == waitCount {
		select {
		case <-timer.C:
			t.Fatal("administrator request did not wait for the pending replacement")
		case <-ticker.C:
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, identity.ErrInvalidAdminSession) {
			t.Fatalf("queued administrator accepted a replaced binding: %v", err)
		}
	case <-timer.C:
		t.Fatal("queued administrator request did not finish")
	}
}

func TestBoundAdminRechecksReservationsBeforeCompletion(t *testing.T) {
	for _, operation := range []string{"create", "reset"} {
		t.Run(operation, func(t *testing.T) {
			f := newBoundAdminFixture(t)
			ctx := context.Background()
			if operation == "create" {
				err := f.db.ReserveLocalAccountCreation(ctx, identity.LocalAccountCreationAttempt{
					Actor: f.actor, AccountID: "reserved-account", OperationID: "bound-create-attempt", OccurredAt: f.clock(),
				})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
					Actor: f.actor, TargetID: "browser-local-b", OperationID: "bound-reset-attempt", OccurredAt: f.clock(),
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := f.db.RevokeBoundSession(ctx, f.record.TokenHash, f.binding, "revoke-before-completion", f.clock()); err != nil {
				t.Fatal(err)
			}
			replacement, _ := browserSessionRecord("completion-replacement", "browser-local-b", f.binding, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			hash, err := identity.HashLocalPassword(rand.Reader, "replacement passphrase")
			if err != nil {
				t.Fatal(err)
			}
			if operation == "create" {
				err = f.db.CompleteLocalAccountCreation(ctx, identity.LocalAccountCreationCompletion{
					Actor: f.actor, Account: identity.LocalAccount{ID: "reserved-account", Username: "reserved-user", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1},
					OperationID: "bound-create-attempt", EventID: "bound-create-outcome", OccurredAt: f.clock(),
				})
			} else {
				err = f.db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
					Actor: f.actor, TargetID: "browser-local-b", NewHash: hash, OperationID: "bound-reset-attempt",
					EventID: "bound-reset-outcome", RevocationEventID: "bound-reset-revocations", OccurredAt: f.clock(),
				})
			}
			if !errors.Is(err, identity.ErrInvalidAdminSession) {
				t.Fatalf("stale bound administrator completed a reserved mutation: %v", err)
			}
			var count int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM identities WHERE id = 'reserved-account'").Scan(&count); err != nil || count != 0 {
				t.Fatal("denied reservation created an account")
			}
			var version int
			if err := f.db.db.QueryRow("SELECT credential_version FROM local_accounts WHERE identity_id = 'browser-local-b'").Scan(&version); err != nil || version != 1 {
				t.Fatal("denied reservation reset a password")
			}
			var completed bool
			table, operationID := "local_account_creation_attempts", "bound-create-attempt"
			if operation == "reset" {
				table, operationID = "password_reset_attempts", "bound-reset-attempt"
			}
			if err := f.db.db.QueryRow("SELECT completed FROM "+table+" WHERE operation_id = ?", operationID).Scan(&completed); err != nil || !completed {
				t.Fatal("denied bound administrator reservation remained reusable")
			}
		})
	}
}

func TestBoundAdminSurvivesRestartAndReportsAuditFailure(t *testing.T) {
	for _, mode := range []string{"restart", "audit", "commit"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundAdminFixture(t)
			ctx := context.Background()
			if mode == "restart" {
				f.reopen(t)
			} else {
				setup := `CREATE TRIGGER reject_bound_admin_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.list' BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
				if mode == "commit" {
					setup = `CREATE TABLE bound_admin_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_bound_admin_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.local_account.list'
BEGIN INSERT INTO bound_admin_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				}
				if _, err := f.db.db.Exec(setup); err != nil {
					t.Fatal(err)
				}
			}
			page, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
				Actor: f.actor, Limit: 10, OperationID: "bound-admin-audit-test", OccurredAt: f.clock(),
			})
			if mode == "restart" {
				if err != nil || len(page.Accounts) != 2 {
					t.Fatalf("bound administrator did not survive restart: %v", err)
				}
			} else if !errors.Is(err, identity.ErrAccountListAudit) || len(page.Accounts) != 0 {
				t.Fatal("administrator audit failure returned account data")
			}
		})
	}
}

func TestBoundAdminStorageFailureAndUnboundIsolation(t *testing.T) {
	f := newBoundAdminFixture(t)
	ctx := context.Background()
	unbound, _ := browserSessionRecord("unbound-admin", "browser-local-a", [32]byte{}, f.clock())
	unbound.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{33}, 16))
	if _, err := f.db.CreateLocalSession(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	boundPage, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
		Actor: f.actor, Limit: 10, OperationID: "missing-bound-admin-table", OccurredAt: f.clock(),
	})
	if err == nil || errors.Is(err, identity.ErrInvalidAdminSession) || len(boundPage.Accounts) != 0 {
		t.Fatal("missing browser storage was hidden or returned administrator data")
	}
	unboundActor := identity.LocalAdminActor{AccountID: unbound.AccountID, SessionID: unbound.SessionID}
	page, err := f.db.ListLocalAccounts(ctx, identity.LocalAccountListRequest{
		Actor: unboundActor, Limit: 10, OperationID: "unbound-admin-without-table", OccurredAt: f.clock(),
	})
	if err != nil || len(page.Accounts) != 2 {
		t.Fatalf("unbound administrator depended on browser storage: %v", err)
	}
}

func TestBoundAdminRejectsBackwardsAndQueuedClocks(t *testing.T) {
	for _, mode := range []string{"before-session", "after-request", "queued"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundAdminFixture(t)
			ctx := context.Background()
			requested := f.record.CreatedAt
			if mode == "before-session" {
				requested = requested.Add(-time.Nanosecond)
				f.current.Store(requested.UnixNano())
			} else if mode == "after-request" {
				f.current.Store(requested.Add(-time.Nanosecond).UnixNano())
			}
			request := identity.LocalAccountListRequest{
				Actor: f.actor, Limit: 10, OperationID: "backwards-bound-admin", OccurredAt: requested,
			}
			var page identity.LocalAccountListResult
			var err error
			if mode == "queued" {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(requested.Add(-time.Nanosecond).UnixNano())
				}, func() error {
					page, err = f.db.ListLocalAccounts(ctx, request)
					return err
				})
			} else {
				page, err = f.db.ListLocalAccounts(ctx, request)
			}
			if !errors.Is(err, ErrInvalidAuditEvent) || len(page.Accounts) != 0 {
				t.Fatalf("bound administrator accepted a backwards clock: %v", err)
			}
		})
	}
}
