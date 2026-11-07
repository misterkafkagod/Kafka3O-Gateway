package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func browserCleanupState(t *testing.T, f browserRetirementFixture) (int, bool, int) {
	t.Helper()
	var bindings, credentials int
	var revoked sql.NullInt64
	var historical []byte
	err := f.fixture.db.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM console_browser_sessions WHERE browser_hash = ?),
  revoked_at_ns, (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?), browser_binding_hash
FROM console_sessions WHERE id = ?`, f.hash[:], f.old.SessionID, f.old.SessionID).
		Scan(&bindings, &revoked, &credentials, &historical)
	if err != nil || !bytes.Equal(historical, f.hash[:]) {
		t.Fatalf("inspect browser cleanup state: %v", err)
	}
	return bindings, revoked.Valid, credentials
}

func TestBrowserBindingCleanupRemovesInactiveSessions(t *testing.T) {
	for _, tt := range []struct {
		source, inactive string
	}{
		{"local", "idle"}, {"local", "absolute"}, {"local", "disabled"}, {"local", "revoked"},
		{"cognito", "idle"}, {"cognito", "absolute"}, {"cognito", "disabled"}, {"cognito", "revoked"},
	} {
		t.Run(tt.source+"/"+tt.inactive, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, tt.source)
			ctx := context.Background()
			switch tt.inactive {
			case "idle":
				f.fixture.current.Store(f.old.IdleExpiresAt.Add(-time.Nanosecond).UnixNano())
				if count, err := f.fixture.db.CleanupBrowserBindings(ctx, "before-idle", f.fixture.clock()); err != nil || count != 0 {
					t.Fatalf("cleanup removed a session before idle expiry: count=%d err=%v", count, err)
				}
				f.fixture.current.Store(f.old.IdleExpiresAt.UnixNano())
			case "absolute":
				if _, err := f.fixture.db.db.Exec("UPDATE console_sessions SET idle_expires_at_ns = absolute_expires_at_ns WHERE id = ?", f.old.SessionID); err != nil {
					t.Fatal(err)
				}
				f.fixture.current.Store(f.old.AbsoluteExpiresAt.Add(-time.Nanosecond).UnixNano())
				if count, err := f.fixture.db.CleanupBrowserBindings(ctx, "before-absolute", f.fixture.clock()); err != nil || count != 0 {
					t.Fatalf("cleanup removed a session before absolute expiry: count=%d err=%v", count, err)
				}
				f.fixture.current.Store(f.old.AbsoluteExpiresAt.UnixNano())
			case "disabled":
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.fixture.db.RevokeBoundSession(ctx, f.old.TokenHash, f.hash, "cleanup-prior-revocation", f.fixture.clock()); err != nil {
					t.Fatal(err)
				}
			}
			if count, err := f.fixture.db.CleanupBrowserBindings(ctx, "cleanup-inactive", f.fixture.clock()); err != nil || count != 1 {
				t.Fatalf("cleanup did not retire the inactive binding: count=%d err=%v", count, err)
			}
			if bindings, revoked, credentials := browserCleanupState(t, f); bindings != 0 || !revoked || credentials != 0 {
				t.Fatalf("cleanup left partial state: bindings=%d revoked=%t credentials=%d", bindings, revoked, credentials)
			}
			events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: browserBindingCleanupAction, Limit: 10})
			if err != nil || len(events) != 1 || events[0].Source != AuditSourceService ||
				events[0].ResourceID != f.old.SessionID || events[0].OperationID != "cleanup-inactive" ||
				events[0].Outcome != AuditSucceeded {
				t.Fatalf("cleanup was not audited: count=%d err=%v", len(events), err)
			}
			encoded, err := json.Marshal(events)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.hash[:]), f.oldToken,
				fixtureRefresh, fixtureRotated, fixtureAccess, f.fixture.provider.Issuer, "opaque|synthetic-subject", "unmapped-group"} {
				if bytes.Contains(encoded, []byte(secret)) || strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("browser cleanup audit exposed sensitive material")
				}
			}
			if tt.inactive == "disabled" {
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			f.fixture.reopen(t)
			if bindings, revoked, credentials := browserCleanupState(t, f); bindings != 0 || !revoked || credentials != 0 {
				t.Fatal("browser cleanup did not persist across restart")
			}
			if _, err := f.fixture.db.UseBoundSession(ctx, f.old.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
				t.Fatalf("cleaned session revived: %v", err)
			}
			if count, err := f.fixture.db.CleanupBrowserBindings(ctx, "cleanup-after-restart", f.fixture.clock()); err != nil || count != 0 {
				t.Fatalf("completed cleanup was repeated: count=%d err=%v", count, err)
			}
			replacement, _ := browserSessionRecord("cleanup-replacement", "browser-local-b", f.hash, f.fixture.clock())
			if principal, err := f.fixture.db.CreateLocalSession(ctx, replacement); err != nil || principal.SessionID != replacement.SessionID {
				t.Fatalf("cleaned browser could not receive a new session: %v", err)
			}
		})
	}
}

func TestBrowserBindingCleanupPreservesActiveAndStaleCognitoSessions(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	f.current.Store(f.freshUntil)
	if count, err := f.db.CleanupBrowserBindings(ctx, "active-stale", f.clock()); err != nil || count != 0 {
		t.Fatalf("stale claims made an active binding eligible: count=%d err=%v", count, err)
	}
	request := f.request(t)
	if _, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding); err != nil {
		t.Fatal(err)
	}
	if count, err := f.db.CleanupBrowserBindings(ctx, "active-refresh", f.clock()); err != nil || count != 0 {
		t.Fatalf("pending refresh made an active binding eligible: count=%d err=%v", count, err)
	}
	var bindings, credentials int
	var revoked sql.NullInt64
	err := f.db.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM console_browser_sessions WHERE browser_hash = ?),
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?), revoked_at_ns
FROM console_sessions WHERE id = ?`, f.binding[:], f.credentials.Principal.SessionID, f.credentials.Principal.SessionID).
		Scan(&bindings, &credentials, &revoked)
	if err != nil || bindings != 1 || credentials != 1 || revoked.Valid {
		t.Fatalf("active pending refresh was changed: %v", err)
	}
}

func TestBrowserBindingCleanupBoundsBatchesAndSkipsActivePrefix(t *testing.T) {
	f := newBrowserRetirementFixture(t, "local")
	ctx, now := context.Background(), f.fixture.clock()
	var early [32]byte
	early[0] = 1
	active, _ := browserSessionRecord("early-active", "browser-local-b", early, now)
	if _, err := f.fixture.db.CreateLocalSession(ctx, active); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < BrowserBindingCleanupBatchSize+1; i++ {
		var hash [32]byte
		hash[0], hash[1] = 255, byte(i+1)
		id := fmt.Sprintf("expired-browser-%03d", i)
		token := sha256.Sum256([]byte("cleanup-token:" + id))
		csrf := sha256.Sum256([]byte("cleanup-csrf:" + id))
		if _, err := f.fixture.db.db.Exec(`INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, browser_binding_hash)
VALUES (?, 'browser-local-b', 1, ?, ?, ?, ?, ?, ?)`,
			id, token[:], csrf[:], now.Add(-time.Hour).UnixNano(), now.UnixNano(), now.Add(time.Hour).UnixNano(), hash[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := f.fixture.db.db.Exec("INSERT INTO console_browser_sessions (browser_hash, session_id) VALUES (?, ?)", hash[:], id); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		operation string
		want      int
	}{{"batch-one", BrowserBindingCleanupBatchSize}, {"batch-two", 1}, {"batch-empty", 0}} {
		if count, err := f.fixture.db.CleanupBrowserBindings(ctx, tt.operation, now); err != nil || count != tt.want {
			t.Fatalf("cleanup %s: count=%d want=%d err=%v", tt.operation, count, tt.want, err)
		}
	}
	var events, unique, bindings int
	if err := f.fixture.db.db.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT event_id) FROM audit_events WHERE action = ?`,
		browserBindingCleanupAction).Scan(&events, &unique); err != nil || events != BrowserBindingCleanupBatchSize+1 || unique != events {
		t.Fatalf("bounded batch audit mismatch: events=%d unique=%d err=%v", events, unique, err)
	}
	if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM console_browser_sessions").Scan(&bindings); err != nil || bindings != 2 {
		t.Fatalf("cleanup did not preserve both active bindings: count=%d err=%v", bindings, err)
	}
	for _, record := range []session.Record{f.old, active} {
		if principal, err := f.fixture.db.UseBoundSession(ctx, record.TokenHash, record.BrowserBindingHash, f.fixture.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != record.SessionID {
			t.Fatalf("active binding was damaged by cleanup: %v", err)
		}
	}
}

func TestBrowserBindingCleanupRollsBackOnFailure(t *testing.T) {
	for _, mode := range []string{"revocation", "ignored revocation", "reversed revocation", "credential deletion", "ignored credential deletion", "audit", "binding deletion", "ignored binding deletion", "reinserted binding", "commit"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "cognito")
			ctx := context.Background()
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			var script string
			switch mode {
			case "revocation":
				script = `CREATE TRIGGER reject_cleanup BEFORE UPDATE OF revoked_at_ns ON console_sessions
BEGIN SELECT RAISE(FAIL, 'revocation unavailable'); END;`
			case "ignored revocation":
				script = `CREATE TRIGGER reject_cleanup BEFORE UPDATE OF revoked_at_ns ON console_sessions
BEGIN SELECT RAISE(IGNORE); END;`
			case "reversed revocation":
				script = `CREATE TRIGGER reject_cleanup AFTER UPDATE OF revoked_at_ns ON console_sessions
WHEN NEW.revoked_at_ns IS NOT NULL
BEGIN UPDATE console_sessions SET revoked_at_ns = NULL WHERE id = NEW.id; END;`
			case "credential deletion":
				script = `CREATE TRIGGER reject_cleanup BEFORE DELETE ON cognito_session_credentials
BEGIN SELECT RAISE(FAIL, 'credential storage unavailable'); END;`
			case "ignored credential deletion":
				script = `CREATE TRIGGER reject_cleanup BEFORE DELETE ON cognito_session_credentials
BEGIN SELECT RAISE(IGNORE); END;`
			case "audit":
				script = `CREATE TRIGGER reject_cleanup BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.browser.binding.cleanup' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			case "binding deletion":
				script = `CREATE TRIGGER reject_cleanup BEFORE DELETE ON console_browser_sessions
BEGIN SELECT RAISE(FAIL, 'binding storage unavailable'); END;`
			case "ignored binding deletion":
				script = `CREATE TRIGGER reject_cleanup BEFORE DELETE ON console_browser_sessions
BEGIN SELECT RAISE(IGNORE); END;`
			case "reinserted binding":
				script = `CREATE TRIGGER reject_cleanup AFTER DELETE ON console_browser_sessions
BEGIN INSERT INTO console_browser_sessions (browser_hash, session_id) VALUES (OLD.browser_hash, OLD.session_id); END;`
			case "commit":
				script = `CREATE TABLE browser_cleanup_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER reject_cleanup AFTER INSERT ON audit_events
WHEN NEW.action = 'console.browser.binding.cleanup'
BEGIN INSERT INTO browser_cleanup_commit_failure (identity_id) VALUES ('missing-identity'); END;`
			}
			if _, err := f.fixture.db.db.Exec(script); err != nil {
				t.Fatal(err)
			}
			count, err := f.fixture.db.CleanupBrowserBindings(ctx, "cleanup-failure", f.fixture.clock())
			if err == nil || count != 0 || (mode == "commit" && !errors.Is(err, ErrBrowserBindingCleanupUnknown)) ||
				(mode == "audit" && !errors.Is(err, session.ErrBrowserSessionAudit)) {
				t.Fatalf("cleanup hid a %s failure: count=%d err=%v", mode, count, err)
			}
			f.fixture.reopen(t)
			if bindings, revoked, credentials := browserCleanupState(t, f); bindings != 1 || revoked || credentials != 1 {
				t.Fatalf("failed cleanup committed partial state: bindings=%d revoked=%t credentials=%d", bindings, revoked, credentials)
			}
			events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: browserBindingCleanupAction, Limit: 10})
			if err != nil || len(events) != 0 {
				t.Fatal("failed cleanup committed an audit")
			}
			if _, err := f.fixture.db.db.Exec("DROP TRIGGER reject_cleanup"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			if principal, err := f.fixture.db.UseBoundSession(ctx, f.old.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, f.protector); err != nil || principal.SessionID != f.old.SessionID {
				t.Fatalf("rollback damaged the old session: %v", err)
			}
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			if count, err := f.fixture.db.CleanupBrowserBindings(ctx, "cleanup-recovered", f.fixture.clock()); err != nil || count != 1 {
				t.Fatalf("cleanup did not recover: count=%d err=%v", count, err)
			}
		})
	}
}

func TestBrowserBindingCleanupRejectsCorruptBindings(t *testing.T) {
	for _, mode := range []string{"mismatched hash", "text hash", "zero hash", "missing session", "missing identity", "invalid source", "invalid lifetime", "future creation"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "local")
			ctx := context.Background()
			switch mode {
			case "mismatched hash":
				other := sha256.Sum256([]byte("another browser binding"))
				if _, err := f.fixture.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = ? WHERE id = ?", other[:], f.old.SessionID); err != nil {
					t.Fatal(err)
				}
			case "text hash":
				if _, err := f.fixture.db.db.Exec("PRAGMA ignore_check_constraints = ON"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.fixture.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = CAST(browser_binding_hash AS TEXT) WHERE id = ?", f.old.SessionID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.fixture.db.db.Exec("PRAGMA ignore_check_constraints = OFF"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
			case "zero hash":
				if _, err := f.fixture.db.db.Exec("UPDATE console_browser_sessions SET browser_hash = zeroblob(32) WHERE session_id = ?", f.old.SessionID); err != nil {
					t.Fatal(err)
				}
			case "missing session", "missing identity":
				if _, err := f.fixture.db.db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
					t.Fatal(err)
				}
				query, arg := "DELETE FROM console_sessions WHERE id = ?", f.old.SessionID
				if mode == "missing identity" {
					query, arg = "DELETE FROM identities WHERE id = ?", f.old.AccountID
				}
				if _, err := f.fixture.db.db.Exec(query, arg); err != nil {
					t.Fatal(err)
				}
				if _, err := f.fixture.db.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
					t.Fatal(err)
				}
			case "invalid source", "invalid lifetime":
				if _, err := f.fixture.db.db.Exec("PRAGMA ignore_check_constraints = ON"); err != nil {
					t.Fatal(err)
				}
				query := "UPDATE console_sessions SET auth_source = 'unknown' WHERE id = ?"
				if mode == "invalid lifetime" {
					query = "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns WHERE id = ?"
				}
				if _, err := f.fixture.db.db.Exec(query, f.old.SessionID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.fixture.db.db.Exec("PRAGMA ignore_check_constraints = OFF"); err != nil {
					t.Fatal(err)
				}
			case "future creation":
				f.fixture.current.Store(f.old.CreatedAt.Add(-time.Second).UnixNano())
			}
			count, err := f.fixture.db.CleanupBrowserBindings(ctx, "corrupt-binding", f.fixture.clock())
			if err == nil || count != 0 {
				t.Fatal("corrupt browser binding was silently removed")
			}
			var bindings, events int
			if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM console_browser_sessions").Scan(&bindings); err != nil || bindings != 1 {
				t.Fatal("corrupt binding was removed")
			}
			if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM audit_events WHERE action = ?", browserBindingCleanupAction).Scan(&events); err != nil || events != 0 {
				t.Fatal("corrupt binding cleanup committed an audit")
			}
		})
	}
}

func TestBrowserBindingCleanupRollsBackEarlierCandidatesOnCorruption(t *testing.T) {
	f, _, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	var firstHash, secondHash [32]byte
	firstHash[0], secondHash[0] = 1, 2
	first, _ := browserSessionRecord("corrupt-batch-first", "browser-local-a", firstHash, f.clock())
	second, _ := browserSessionRecord("corrupt-batch-second", "browser-local-b", secondHash, f.clock())
	for _, record := range []session.Record{first, second} {
		if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	other := sha256.Sum256([]byte("corrupt historical binding"))
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id IN ('browser-local-a', 'browser-local-b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = ? WHERE id = ?", other[:], second.SessionID); err != nil {
		t.Fatal(err)
	}
	if count, err := f.db.CleanupBrowserBindings(ctx, "corrupt-batch", f.clock()); err == nil || count != 0 {
		t.Fatal("corrupt second candidate did not abort cleanup")
	}
	var binding, audits int
	var revoked sql.NullInt64
	if err := f.db.db.QueryRow(`SELECT revoked_at_ns,
  (SELECT COUNT(*) FROM console_browser_sessions WHERE session_id = ?),
  (SELECT COUNT(*) FROM audit_events WHERE action = ?)
FROM console_sessions WHERE id = ?`, first.SessionID, browserBindingCleanupAction, first.SessionID).
		Scan(&revoked, &binding, &audits); err != nil || revoked.Valid || binding != 1 || audits != 0 {
		t.Fatalf("corrupt batch committed an earlier retirement: %v", err)
	}
}

func TestBrowserBindingCleanupRechecksQueueClockAndCancellation(t *testing.T) {
	f := newBrowserRetirementFixture(t, "local")
	ctx := context.Background()
	requested := f.fixture.clock()
	var count int
	err := runQueuedDatabaseOperation(t, f.fixture.db, func() {
		f.fixture.current.Store(f.old.IdleExpiresAt.UnixNano())
	}, func() error {
		var err error
		count, err = f.fixture.db.CleanupBrowserBindings(ctx, "queued-browser-cleanup", requested)
		return err
	})
	if err != nil || count != 1 {
		t.Fatalf("queued cleanup missed expiry: count=%d err=%v", count, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.fixture.db.CleanupBrowserBindings(cancelled, "cancelled", f.fixture.clock()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup was not reported: %v", err)
	}
	for _, tt := range []struct {
		ctx       context.Context
		operation string
		at        time.Time
	}{
		{nil, "valid", f.fixture.clock()}, {ctx, "", f.fixture.clock()}, {ctx, " ", f.fixture.clock()},
		{ctx, strings.Repeat("x", 121), f.fixture.clock()}, {ctx, "invalid\noperation", f.fixture.clock()},
		{ctx, "invalid-clock", time.Time{}},
	} {
		if _, err := f.fixture.db.CleanupBrowserBindings(tt.ctx, tt.operation, tt.at); !errors.Is(err, ErrBrowserBindingCleanupConfig) {
			t.Fatalf("invalid cleanup request was accepted: %v", err)
		}
	}
	if _, err := f.fixture.db.CleanupBrowserBindings(ctx, "future-request", f.fixture.clock().Add(time.Second)); !errors.Is(err, ErrInvalidAuditEvent) {
		t.Fatalf("backwards transaction clock was accepted: %v", err)
	}
	f.fixture.db.now = func() time.Time { return time.Time{} }
	if _, err := f.fixture.db.CleanupBrowserBindings(ctx, "invalid-store-clock", f.fixture.clock()); !errors.Is(err, ErrInvalidAuditEvent) {
		t.Fatalf("invalid store clock was accepted: %v", err)
	}
}

func TestBrowserBindingCleanupReportsClockRegressionAndUnknownCommit(t *testing.T) {
	for _, badCall := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("clock-call-%d", badCall), func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "cognito")
			ctx := context.Background()
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			requested := f.fixture.clock()
			calls := 0
			f.fixture.db.now = func() time.Time {
				calls++
				if calls == badCall {
					return requested.Add(-time.Second)
				}
				return requested
			}
			count, err := f.fixture.db.CleanupBrowserBindings(ctx, "regressing-cleanup", requested)
			if count != 0 || !errors.Is(err, ErrInvalidAuditEvent) ||
				(badCall == 4 && !errors.Is(err, ErrBrowserBindingCleanupUnknown)) {
				t.Fatalf("clock regression was not surfaced: count=%d err=%v", count, err)
			}
			bindings, revoked, credentials := browserCleanupState(t, f)
			if badCall < 4 && (bindings != 1 || revoked || credentials != 1) {
				t.Fatal("pre-commit clock regression committed cleanup")
			}
			if badCall == 4 && (bindings != 0 || !revoked || credentials != 0) {
				t.Fatal("post-commit regression did not report the committed outcome as unknown")
			}
		})
	}
}

func TestBrowserBindingCleanupSerializesWithReplacementAndLogout(t *testing.T) {
	for _, mode := range []string{"replacement", "logout"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "cognito")
			ctx := context.Background()
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			replacement, _ := browserSessionRecord("cleanup-race", "browser-local-b", f.hash, f.fixture.clock())
			start := make(chan struct{})
			cleanupDone, otherDone := make(chan error, 1), make(chan error, 1)
			var wait sync.WaitGroup
			wait.Add(2)
			go func() {
				defer wait.Done()
				<-start
				_, err := f.fixture.db.CleanupBrowserBindings(ctx, "race-cleanup", f.fixture.clock())
				cleanupDone <- err
			}()
			go func() {
				defer wait.Done()
				<-start
				var err error
				if mode == "replacement" {
					_, err = f.fixture.db.CreateLocalSession(ctx, replacement)
				} else {
					err = f.fixture.db.RevokeBoundSession(ctx, f.old.TokenHash, f.hash, "race-logout", f.fixture.clock())
				}
				otherDone <- err
			}()
			close(start)
			wait.Wait()
			if err := <-cleanupDone; err != nil {
				t.Fatalf("concurrent cleanup failed: %v", err)
			}
			if err := <-otherDone; err != nil && !(mode == "logout" && err == session.ErrInvalidSession) {
				t.Fatalf("concurrent %s failed: %v", mode, err)
			}
			bindings, revoked, credentials := browserCleanupState(t, f)
			if !revoked || credentials != 0 {
				t.Fatal("concurrent cleanup left an old live credential")
			}
			if mode == "replacement" {
				if bindings != 1 {
					t.Fatal("cleanup removed the replacement binding")
				}
				if principal, err := f.fixture.db.UseBoundSession(ctx, replacement.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != replacement.SessionID {
					t.Fatalf("cleanup damaged the replacement: %v", err)
				}
			} else if bindings != 0 {
				t.Fatal("cleanup did not remove the logged-out binding")
			}
			var retirements int
			if err := f.fixture.db.db.QueryRow(`SELECT COUNT(*) FROM audit_events
WHERE action IN (?, ?) AND resource_id = ?`, browserBindingCleanupAction, browserSessionRetirementAction, f.old.SessionID).
				Scan(&retirements); err != nil || retirements != 1 {
				t.Fatalf("old session was not retired exactly once: count=%d err=%v", retirements, err)
			}
		})
	}
}

func TestBrowserBindingCleanupPreventsInFlightRefreshRevival(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	f.mu.Lock()
	f.mode = "refresh rotated"
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f.refreshEntered, f.refreshRelease = entered, release
	f.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	ctx := context.Background()
	type exchangeResult struct {
		result cognito.RefreshExchangeResult
		err    error
	}
	done := make(chan exchangeResult, 1)
	go func() {
		result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
		done <- exchangeResult{result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	if count, err := f.db.CleanupBrowserBindings(ctx, "cleanup-during-refresh", f.clock()); err != nil || count != 1 {
		t.Fatalf("cleanup missed in-flight inactive session: count=%d err=%v", count, err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	close(release)
	var exchange exchangeResult
	select {
	case exchange = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not finish")
	}
	if exchange.err != nil {
		t.Fatalf("fixture provider exchange failed: %v", exchange.err)
	}
	proof, err := exchange.result.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.CompleteBoundCognitoRefresh(ctx, cognito.RefreshCompletionRequest{
		Refresh: proof, Protector: f.protector, OccurredAt: f.clock(),
	}, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("in-flight refresh restored a cleaned session: %v", err)
	}
	var credentials, bindings int
	if err := f.db.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?),
  (SELECT COUNT(*) FROM console_browser_sessions WHERE browser_hash = ?)`,
		f.credentials.Principal.SessionID, f.binding[:]).Scan(&credentials, &bindings); err != nil || credentials != 0 || bindings != 0 {
		t.Fatal("in-flight refresh restored a binding or provider credential")
	}
}
