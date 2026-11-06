package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundLogoutRevokesCurrentLocalAndCognitoSessions(t *testing.T) {
	for _, source := range []session.AuthenticationSource{session.SourceLocal, session.SourceCognito} {
		t.Run(string(source), func(t *testing.T) {
			f, binding, marker := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, token := browserSessionRecord("logout-local", "browser-local-a", binding, f.clock())
			var principal session.Principal
			var err error
			if source == session.SourceCognito {
				cognitoRecord, cognitoToken, _ := browserCognitoRecord(t, f, "logout-cognito", binding)
				record, token = cognitoRecord.Session, cognitoToken
				principal, err = f.db.CreateCognitoSession(ctx, cognitoRecord)
			} else {
				principal, err = f.db.CreateLocalSession(ctx, record)
			}
			if err != nil {
				t.Fatal(err)
			}
			wrong := sha256.Sum256([]byte("wrong browser"))
			for _, browserHash := range [][32]byte{{}, wrong} {
				if err := f.db.RevokeBoundSession(ctx, record.TokenHash, browserHash, "wrong-bound-logout", f.clock()); err != session.ErrInvalidSession {
					t.Fatal("wrong browser revoked the current session")
				}
				if found, err := f.db.FindBoundCognitoSessionForLogout(ctx, record.TokenHash, browserHash); err != session.ErrInvalidSession || found.SessionID != "" {
					t.Fatal("wrong browser resolved a Cognito logout principal")
				}
			}
			if err := f.db.RevokeSession(ctx, record.TokenHash, "unbound-logout", f.clock()); err != session.ErrInvalidSession {
				t.Fatal("unbound revocation accepted a bound session")
			}
			if _, err := f.db.FindCognitoSessionForLogout(ctx, record.TokenHash); err != session.ErrInvalidSession {
				t.Fatal("unbound Cognito logout lookup accepted a bound session")
			}
			if source == session.SourceCognito {
				found, err := f.db.FindBoundCognitoSessionForLogout(ctx, record.TokenHash, binding)
				if err != nil || found.SessionID != principal.SessionID || found.AccountID != principal.AccountID || found.CSRFHash != record.CSRFHash {
					t.Fatalf("bound Cognito logout principal was unavailable: %v", err)
				}
			} else if _, err := f.db.FindBoundCognitoSessionForLogout(ctx, record.TokenHash, binding); err != session.ErrInvalidSession {
				t.Fatal("local session was identified as a Cognito logout")
			}
			if err := f.db.RevokeBoundSession(ctx, record.TokenHash, binding, "correct-bound-logout", f.clock()); err != nil {
				t.Fatalf("current bound session could not be revoked: %v", err)
			}
			if err := f.db.RevokeBoundSession(ctx, record.TokenHash, binding, "repeat-bound-logout", f.clock()); err != session.ErrInvalidSession {
				t.Fatal("revoked bound session was revoked twice")
			}
			if _, err := f.db.FindBoundCognitoSessionForLogout(ctx, record.TokenHash, binding); err != session.ErrInvalidSession {
				t.Fatal("revoked session remained available for Cognito logout")
			}
			var revoked sql.NullInt64
			var credentials int
			if err := f.db.db.QueryRow("SELECT revoked_at_ns FROM console_sessions WHERE id = ?", record.SessionID).Scan(&revoked); err != nil || !revoked.Valid {
				t.Fatal("bound logout did not persist revocation")
			}
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", record.SessionID).Scan(&credentials); err != nil || credentials != 0 {
				t.Fatal("bound logout retained provider credentials")
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
			if err != nil || len(events) != 1 || events[0].ActorID != principal.AccountID || events[0].Outcome != AuditSucceeded ||
				string(events[0].Source) != string(source) {
				t.Fatal("bound logout was not audited exactly once")
			}
			for _, secret := range []string{marker, token, fixtureRefresh, fixtureAccess, f.provider.Issuer, "opaque|synthetic-subject"} {
				if strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("bound logout audit exposed credentials or verified claims")
				}
			}
			for _, path := range []string{f.path, f.path + "-wal"} {
				data, err := os.ReadFile(path)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				if bytes.Contains(data, []byte(marker)) || bytes.Contains(data, []byte(token)) || bytes.Contains(data, []byte(fixtureRefresh)) {
					t.Fatal("bound logout stored plaintext browser or session credentials")
				}
			}
			f.reopen(t)
			if _, err := f.db.UseBoundSession(ctx, record.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
				t.Fatal("bound logout did not survive restart")
			}
			replacement, _ := browserSessionRecord("logout-replacement", "browser-local-b", binding, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
				t.Fatalf("successful logout did not release its browser: %v", err)
			}
			if f.refreshCalls.Load() != 0 {
				t.Fatal("local logout contacted the provider")
			}
		})
	}
}

func TestBoundLogoutAllowsUnavailableCognitoSessions(t *testing.T) {
	for _, mode := range []string{"stale-claims", "disabled-identity", "expired-session", "tampered-credential", "missing-credential"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, _, protector := browserCognitoRecord(t, f, "unavailable-"+mode, binding)
			principal, err := f.db.CreateCognitoSession(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "stale-claims":
				f.current.Store(f.clock().Add(5 * time.Minute).UnixNano())
			case "disabled-identity":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "expired-session":
				f.current.Store(record.Session.IdleExpiresAt.UnixNano())
			case "tampered-credential":
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET encrypted_refresh_token = zeroblob(32) WHERE session_id = ?", record.Session.SessionID); err != nil {
					t.Fatal(err)
				}
			case "missing-credential":
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", record.Session.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			if resolved, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, protector); err == nil || resolved.SessionID != "" {
				t.Fatal("unavailable Cognito session was usable")
			}
			found, err := f.db.FindBoundCognitoSessionForLogout(ctx, record.Session.TokenHash, binding)
			if err != nil || found.SessionID != principal.SessionID || found.CSRFHash != record.Session.CSRFHash {
				t.Fatalf("unavailable Cognito session could not be prepared for local logout: %v", err)
			}
			if err := f.db.RevokeBoundSession(ctx, record.Session.TokenHash, binding, "unavailable-bound-logout", f.clock()); err != nil {
				t.Fatalf("unavailable Cognito session could not be locally revoked: %v", err)
			}
			var retained int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", record.Session.SessionID).Scan(&retained); err != nil || retained != 0 {
				t.Fatal("unavailable Cognito logout retained credentials")
			}
			if f.refreshCalls.Load() != 0 {
				t.Fatal("unavailable Cognito logout contacted the provider")
			}
		})
	}
}

func TestBoundLogoutCannotRevokeReplacedSession(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	old, _, _ := browserCognitoRecord(t, f, "logout-old", binding)
	oldPrincipal, err := f.db.CreateCognitoSession(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", oldPrincipal.AccountID); err != nil {
		t.Fatal(err)
	}
	replacement, _ := browserSessionRecord("logout-new", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", oldPrincipal.AccountID); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.FindBoundCognitoSessionForLogout(ctx, old.Session.TokenHash, binding); err != session.ErrInvalidSession || principal.SessionID != "" {
		t.Fatal("old session remained available for logout after replacement")
	}
	if err := f.db.RevokeBoundSession(ctx, old.Session.TokenHash, binding, "old-session-logout", f.clock()); err != session.ErrInvalidSession {
		t.Fatal("old session revoked after replacement")
	}
	if principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != replacement.SessionID {
		t.Fatal("old logout affected the replacement session")
	}
}

func TestBoundLogoutFailuresDoNotPartiallyCommit(t *testing.T) {
	for _, mode := range []string{"audit", "commit", "ignored-update", "ignored-credential-delete", "missing-table", "backwards-clock"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, marker := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, token, _ := browserCognitoRecord(t, f, "failure-"+mode, binding)
			if _, err := f.db.CreateCognitoSession(ctx, record); err != nil {
				t.Fatal(err)
			}
			var setup string
			switch mode {
			case "audit":
				setup = `CREATE TRIGGER reject_bound_logout BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.logout' BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
			case "commit":
				setup = `CREATE TABLE bound_logout_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_bound_logout_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.session.logout'
BEGIN INSERT INTO bound_logout_commit_failure (identity_id) VALUES ('missing-identity'); END;`
			case "ignored-update":
				setup = `CREATE TRIGGER ignore_bound_logout BEFORE UPDATE OF revoked_at_ns ON console_sessions
BEGIN SELECT RAISE(IGNORE); END;`
			case "ignored-credential-delete":
				setup = `CREATE TRIGGER ignore_bound_credential_delete BEFORE DELETE ON cognito_session_credentials
BEGIN SELECT RAISE(IGNORE); END;`
			case "missing-table":
				setup = "DROP TABLE console_browser_sessions"
			case "backwards-clock":
				f.current.Store(record.Session.CreatedAt.Add(-time.Nanosecond).UnixNano())
			}
			if setup != "" {
				if _, err := f.db.db.Exec(setup); err != nil {
					t.Fatal(err)
				}
			}
			err := f.db.RevokeBoundSession(ctx, record.Session.TokenHash, binding, "failed-bound-logout", record.Session.CreatedAt)
			if err == nil || err == session.ErrInvalidSession {
				t.Fatal("logout failure was reported as success or an ordinary invalid session")
			}
			if mode == "commit" && !errors.Is(err, session.ErrBrowserSessionUnknown) {
				t.Fatal("uncertain logout commit was not reported")
			}
			if mode == "backwards-clock" && !errors.Is(err, ErrInvalidAuditEvent) {
				t.Fatal("backwards logout clock was not reported")
			}
			for _, secret := range []string{marker, token, fixtureRefresh, fixtureAccess} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("logout failure exposed credentials")
				}
			}
			var revoked sql.NullInt64
			var retained int
			if err := f.db.db.QueryRow("SELECT revoked_at_ns FROM console_sessions WHERE id = ?", record.Session.SessionID).Scan(&revoked); err != nil || revoked.Valid {
				t.Fatal("failed logout persisted revocation")
			}
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", record.Session.SessionID).Scan(&retained); err != nil || retained != 1 {
				t.Fatal("failed logout removed provider credentials")
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
			if err != nil || len(events) != 0 {
				t.Fatal("failed logout committed a success audit")
			}
		})
	}
}

func TestBoundLogoutStorageFailureDoesNotAffectUnboundSessions(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	bound, _, _ := browserCognitoRecord(t, f, "storage-bound", binding)
	unbound, _, _ := browserCognitoRecord(t, f, "storage-unbound", [32]byte{})
	if _, err := f.db.CreateCognitoSession(ctx, bound); err != nil {
		t.Fatal(err)
	}
	unboundPrincipal, err := f.db.CreateCognitoSession(ctx, unbound)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.FindBoundCognitoSessionForLogout(ctx, bound.Session.TokenHash, binding); err == nil || err == session.ErrInvalidSession || principal.SessionID != "" {
		t.Fatal("missing binding storage was hidden during bound logout lookup")
	}
	if err := f.db.RevokeBoundSession(ctx, bound.Session.TokenHash, binding, "storage-bound-logout", f.clock()); err == nil || err == session.ErrInvalidSession {
		t.Fatal("missing binding storage was hidden during bound revocation")
	}
	if principal, err := f.db.FindCognitoSessionForLogout(ctx, unbound.Session.TokenHash); err != nil || principal.SessionID != unboundPrincipal.SessionID {
		t.Fatalf("unbound logout lookup depended on binding storage: %v", err)
	}
	if err := f.db.RevokeSession(ctx, unbound.Session.TokenHash, "storage-unbound-logout", f.clock()); err != nil {
		t.Fatalf("unbound logout depended on binding storage: %v", err)
	}
}

func TestBoundLogoutRechecksQueuedClock(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, _ := browserSessionRecord("queued-logout", "browser-local-a", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	requested := f.clock()
	err := runQueuedDatabaseOperation(t, f.db, func() {
		f.current.Store(requested.Add(-time.Nanosecond).UnixNano())
	}, func() error {
		return f.db.RevokeBoundSession(ctx, record.TokenHash, binding, "queued-bound-logout", requested)
	})
	if !errors.Is(err, ErrInvalidAuditEvent) {
		t.Fatalf("queued logout accepted a backwards clock: %v", err)
	}
	var revoked sql.NullInt64
	if err := f.db.db.QueryRow("SELECT revoked_at_ns FROM console_sessions WHERE id = ?", record.SessionID).Scan(&revoked); err != nil || revoked.Valid {
		t.Fatal("queued backwards-clock logout revoked a session")
	}
}
