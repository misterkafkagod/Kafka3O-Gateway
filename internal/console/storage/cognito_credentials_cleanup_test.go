package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func cleanupFixtureState(t *testing.T, f *refreshingSessionFixture) (bool, sql.NullInt64) {
	t.Helper()
	var retained bool
	var revoked sql.NullInt64
	if err := f.db.db.QueryRow(`SELECT EXISTS(
SELECT 1 FROM cognito_session_credentials WHERE session_id = console_sessions.id
), revoked_at_ns FROM console_sessions WHERE id = ?`, f.credentials.Principal.SessionID).Scan(&retained, &revoked); err != nil {
		t.Fatal(err)
	}
	return retained, revoked
}

func TestCognitoCredentialCleanupHonorsSessionExpiryAndRestart(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	idle := f.credentials.Principal.IdleExpiresAt
	f.current.Store(idle.UnixNano() - 1)
	if count, err := f.db.CleanupCognitoCredentials(ctx, "before-expiry", f.clock()); err != nil || count != 0 {
		t.Fatalf("cleanup removed a stale but active session: count=%d err=%v", count, err)
	}
	f.current.Store(idle.UnixNano())
	if count, err := f.db.CleanupCognitoCredentials(ctx, "at-expiry", f.clock()); err != nil || count != 1 {
		t.Fatalf("cleanup missed exact idle expiry: count=%d err=%v", count, err)
	}
	retained, revoked := cleanupFixtureState(t, f)
	if retained || !revoked.Valid || revoked.Int64 != idle.UnixNano() {
		t.Fatal("expired credentials were retained or their session was not revoked")
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.credentials.cleanup", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Source != AuditSourceService ||
		events[0].ResourceID != f.credentials.Principal.SessionID || events[0].Outcome != AuditSucceeded {
		t.Fatalf("cleanup was not audited: count=%d err=%v", len(events), err)
	}
	redacted := fmt.Sprint(events)
	for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, f.credentials.Token,
		f.credentials.CSRFToken, f.provider.Issuer, "opaque|synthetic-subject", "unmapped-group"} {
		if strings.Contains(redacted, secret) {
			t.Fatal("cleanup audit exposed a credential or provider identity")
		}
	}
	f.reopen(t)
	retained, revoked = cleanupFixtureState(t, f)
	if retained || !revoked.Valid {
		t.Fatal("cleanup did not persist across restart")
	}
	if count, err := f.db.CleanupCognitoCredentials(ctx, "after-restart", f.clock()); err != nil || count != 0 {
		t.Fatalf("cleanup repeated an already completed deletion: count=%d err=%v", count, err)
	}
}

func TestCognitoCredentialCleanupHonorsAbsoluteExpiry(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, `UPDATE console_sessions
SET idle_expires_at_ns = absolute_expires_at_ns WHERE id = ?`, f.credentials.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	absolute := f.credentials.Principal.AbsoluteExpiresAt
	f.current.Store(absolute.UnixNano() - 1)
	if count, err := f.db.CleanupCognitoCredentials(ctx, "before-absolute", f.clock()); err != nil || count != 0 {
		t.Fatalf("cleanup ran before absolute expiry: count=%d err=%v", count, err)
	}
	f.current.Store(absolute.UnixNano())
	if count, err := f.db.CleanupCognitoCredentials(ctx, "at-absolute", f.clock()); err != nil || count != 1 {
		t.Fatalf("cleanup missed exact absolute expiry: count=%d err=%v", count, err)
	}
}

func TestCognitoCredentialCleanupPreservesPendingRefreshAndAudit(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	ctx := context.Background()
	if _, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, f.credentials, f.protector, f.clock())); err != nil {
		t.Fatal(err)
	}
	if count, err := f.db.CleanupCognitoCredentials(ctx, "pending-refresh", f.clock()); err != nil || count != 0 {
		t.Fatalf("cleanup removed an active pending refresh: count=%d err=%v", count, err)
	}
	retained, revoked := cleanupFixtureState(t, f)
	if !retained || revoked.Valid {
		t.Fatal("cleanup revoked an active pending refresh")
	}
	f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
	if count, err := f.db.CleanupCognitoCredentials(ctx, "expired-refresh", f.clock()); err != nil || count != 1 {
		t.Fatalf("cleanup missed expired pending refresh: count=%d err=%v", count, err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
		t.Fatalf("cleanup removed refresh replay-prevention audit: count=%d err=%v", len(events), err)
	}
}

func TestCognitoCredentialCleanupRevokesDisabledAndPreviouslyRevokedSessions(t *testing.T) {
	for _, mode := range []string{"disabled", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			f := newRefreshingSessionFixture(t, "refresh rotated")
			ctx := context.Background()
			if mode == "disabled" {
				if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET revoked_at_ns = ? WHERE id = ?",
				f.clock().UnixNano(), f.credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			if count, err := f.db.CleanupCognitoCredentials(ctx, "inactive", f.clock()); err != nil || count != 1 {
				t.Fatalf("cleanup missed inactive credentials: count=%d err=%v", count, err)
			}
			retained, revoked := cleanupFixtureState(t, f)
			if retained || !revoked.Valid {
				t.Fatal("inactive session was not revoked and cleaned")
			}
			if mode == "disabled" {
				if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.sessions.Resolve(ctx, f.credentials.Token); err != session.ErrInvalidSession {
				t.Fatalf("cleanup allowed inactive session revival: %v", err)
			}
		})
	}
}

func TestCognitoCredentialCleanupProtectsInFlightRefreshAndLogout(t *testing.T) {
	t.Run("refresh", func(t *testing.T) {
		f := newRefreshingSessionFixture(t, "refresh rotated")
		f.makeStale()
		entered, release := f.pauseProvider(t)
		done := make(chan error, 1)
		go func() {
			_, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("provider refresh did not start")
		}
		f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
		if count, err := f.db.CleanupCognitoCredentials(context.Background(), "during-refresh", f.clock()); err != nil || count != 1 {
			t.Fatalf("cleanup missed an expired in-flight session: count=%d err=%v", count, err)
		}
		close(release)
		if err := <-done; !errors.Is(err, session.ErrCognitoRefreshUnavailable) {
			t.Fatalf("in-flight refresh revived a cleaned session: %v", err)
		}
		if retained, revoked := cleanupFixtureState(t, f); retained || !revoked.Valid {
			t.Fatal("in-flight refresh restored cleaned credentials")
		}
	})
	t.Run("logout", func(t *testing.T) {
		f := newRefreshingSessionFixture(t, "refresh rotated")
		f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
		ctx := context.Background()
		start := make(chan struct{})
		cleanupDone, logoutDone := make(chan error, 1), make(chan error, 1)
		go func() {
			<-start
			_, err := f.db.CleanupCognitoCredentials(ctx, "concurrent-logout", f.clock())
			cleanupDone <- err
		}()
		go func() {
			<-start
			logoutDone <- f.sessions.Revoke(ctx, f.credentials.Token)
		}()
		close(start)
		if err := <-cleanupDone; err != nil {
			t.Fatalf("concurrent cleanup failed: %v", err)
		}
		if err := <-logoutDone; err != nil && err != session.ErrInvalidSession {
			t.Fatalf("concurrent logout failed unexpectedly: %v", err)
		}
		if retained, revoked := cleanupFixtureState(t, f); retained || !revoked.Valid {
			t.Fatal("cleanup and logout left an active credential")
		}
	})
}

func TestCognitoCredentialCleanupBoundsBatchesAndLeavesLocalSessions(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	passwordHash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: passwordHash,
		Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	local, err := f.sessions.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	now := f.clock()
	for i := 0; i < CognitoCleanupBatchSize+1; i++ {
		id := fmt.Sprintf("expired-%03d", i)
		token := sha256.Sum256([]byte("cleanup-test-" + id))
		if _, err := f.db.db.ExecContext(ctx, `INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, auth_source)
VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'cognito')`, id, f.credentials.Principal.AccountID, token[:], token[:],
			now.Add(-time.Hour).UnixNano(), now.UnixNano(), now.Add(time.Hour).UnixNano()); err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.db.ExecContext(ctx, `INSERT INTO cognito_session_credentials
(session_id, identity_id, exchange_operation_id, original_nonce_hash, audience_json, groups_json,
issued_at_ns, token_expires_at_ns, claims_fresh_until_ns, refresh_generation, encrypted_refresh_token)
SELECT ?, identity_id, ?, original_nonce_hash, audience_json, groups_json, issued_at_ns, token_expires_at_ns,
claims_fresh_until_ns, refresh_generation, encrypted_refresh_token
FROM cognito_session_credentials WHERE session_id = ?`, id, "exchange-"+id, f.credentials.Principal.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		id    string
		count int
	}{{"first-batch", CognitoCleanupBatchSize}, {"second-batch", 1}, {"empty-batch", 0}} {
		if count, err := f.db.CleanupCognitoCredentials(ctx, tt.id, now); err != nil || count != tt.count {
			t.Fatalf("bounded batch %s: count=%d want=%d err=%v", tt.id, count, tt.count, err)
		}
	}
	var cleaned int
	if err := f.db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events
WHERE action = 'console.cognito.credentials.cleanup' AND outcome = 'succeeded'`).Scan(&cleaned); err != nil ||
		cleaned != CognitoCleanupBatchSize+1 {
		t.Fatalf("bounded cleanup audit count=%d err=%v", cleaned, err)
	}
	if retained, revoked := cleanupFixtureState(t, f); !retained || revoked.Valid {
		t.Fatal("bounded cleanup affected a valid Cognito session")
	}
	if _, err := f.sessions.Resolve(ctx, local.Token); err != nil {
		t.Fatalf("bounded cleanup affected a local session: %v", err)
	}
}

func TestCognitoCredentialCleanupFailsClosedOnAuditAndCommitFailure(t *testing.T) {
	for _, mode := range []string{"audit", "commit"} {
		t.Run(mode, func(t *testing.T) {
			f := newRefreshingSessionFixture(t, "refresh rotated")
			ctx := context.Background()
			f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
			trigger := `CREATE TRIGGER reject_cleanup BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.credentials.cleanup'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			if mode == "commit" {
				trigger = `CREATE TABLE cleanup_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER reject_cleanup AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.credentials.cleanup'
BEGIN INSERT INTO cleanup_commit_failure (identity_id) VALUES ('missing-identity'); END;`
			}
			if _, err := f.db.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			count, err := f.db.CleanupCognitoCredentials(ctx, "failed-cleanup", f.clock())
			if err == nil || count != 0 || (mode == "commit" && !errors.Is(err, ErrCognitoCleanupUnknown)) {
				t.Fatalf("cleanup hid %s failure: count=%d err=%v", mode, count, err)
			}
			if retained, revoked := cleanupFixtureState(t, f); !retained || revoked.Valid {
				t.Fatal("failed cleanup committed a partial credential deletion")
			}
			if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_cleanup"); err != nil {
				t.Fatal(err)
			}
			if count, err := f.db.CleanupCognitoCredentials(ctx, "recovered-cleanup", f.clock()); err != nil || count != 1 {
				t.Fatalf("cleanup did not recover after %s failure: count=%d err=%v", mode, count, err)
			}
		})
	}
}

func TestCognitoCredentialCleanupChecksQueueClockAndCancellation(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	requested := f.clock()
	var count int
	err := runQueuedDatabaseOperation(t, f.db, func() {
		f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
	}, func() error {
		var err error
		count, err = f.db.CleanupCognitoCredentials(ctx, "queued-cleanup", requested)
		return err
	})
	if err != nil || count != 1 {
		t.Fatalf("queued cleanup did not recheck transaction time: count=%d err=%v", count, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.db.CleanupCognitoCredentials(cancelled, "cancelled", f.clock()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup was not reported: %v", err)
	}
	for _, tt := range []struct {
		ctx       context.Context
		operation string
		at        time.Time
	}{
		{nil, "valid", f.clock()},
		{ctx, "", f.clock()},
		{ctx, "   ", f.clock()},
		{ctx, strings.Repeat("x", 121), f.clock()},
		{ctx, "invalid\noperation", f.clock()},
		{ctx, "invalid-clock", time.Time{}},
	} {
		if _, err := f.db.CleanupCognitoCredentials(tt.ctx, tt.operation, tt.at); !errors.Is(err, ErrCognitoCleanupConfig) {
			t.Fatalf("invalid cleanup input was accepted: %v", err)
		}
	}
	f.db.now = func() time.Time { return time.Time{} }
	if _, err := f.db.CleanupCognitoCredentials(ctx, "invalid-store-clock", f.clock()); !errors.Is(err, ErrInvalidAuditEvent) {
		t.Fatalf("invalid transaction clock was accepted: %v", err)
	}
}
