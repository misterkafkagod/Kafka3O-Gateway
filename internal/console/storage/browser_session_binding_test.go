package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func browserSessionRecord(name, account string, binding [32]byte, now time.Time) (session.Record, string) {
	rawToken := sha256.Sum256([]byte("browser-session-token:" + name))
	csrf := sha256.Sum256([]byte("browser-session-csrf:" + name))
	return session.Record{
		SessionID: "browser-session-" + name, AccountID: account, CredentialVersion: 1,
		TokenHash: sha256.Sum256(rawToken[:]), CSRFHash: csrf, BrowserBindingHash: binding,
		CreatedAt: now, IdleExpiresAt: now.Add(30 * time.Minute), AbsoluteExpiresAt: now.Add(8 * time.Hour),
		EventID: "browser-session-event-" + name,
	}, base64.RawURLEncoding.EncodeToString(rawToken[:])
}

func newBrowserSessionFixture(t *testing.T) (*cognitoCodeFixture, [32]byte, string) {
	t.Helper()
	return newBrowserSessionFixtureWithAuthentication(t, cognito.ClientAuthenticationPublic)
}

func newBrowserSessionFixtureWithAuthentication(t *testing.T, authentication cognito.ClientAuthentication) (*cognitoCodeFixture, [32]byte, string) {
	t.Helper()
	f := newCognitoCodeFixture(t, "success", authentication)
	if _, err := f.db.db.Exec(`INSERT INTO identities (id, enabled) VALUES ('browser-local-a', 1), ('browser-local-b', 1);
INSERT INTO local_accounts (identity_id, username, password_hash, must_change_password, credential_version)
VALUES ('browser-local-a', 'browser-local-a', 'synthetic-hash', 0, 1),
       ('browser-local-b', 'browser-local-b', 'synthetic-hash', 0, 1)`); err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte{27}, 32)
	return f, sha256.Sum256(raw), base64.RawURLEncoding.EncodeToString(raw)
}

func browserCognitoRecord(t *testing.T, f *cognitoCodeFixture, name string, binding [32]byte) (session.CognitoRecord, string, *cognito.RefreshTokenProtector) {
	t.Helper()
	var exchange cognito.CodeExchangeResult
	if binding == ([32]byte{}) {
		_, exchange = cognitoSessionExchange(t, f)
	} else {
		_, exchange = boundCognitoSessionExchange(t, f, binding)
	}
	_, protector := cognitoSessionTools(t, f)
	var proof cognito.VerifiedCodeExchange
	var err error
	if binding == ([32]byte{}) {
		proof, err = exchange.ForSession(protector, f.clock())
	} else {
		proof, err = exchange.ForBoundSession(protector, f.clock(), binding)
	}
	if err != nil {
		t.Fatal(err)
	}
	record, token := browserSessionRecord(name, "", binding, f.clock())
	return session.CognitoRecord{
		Session: record, CandidateAccount: "browser-cognito-" + name, Exchange: proof, Protector: protector,
	}, token, protector
}

func TestBrowserSessionLocalConflictAndRevocation(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	first, _ := browserSessionRecord("first", "browser-local-a", binding, f.clock())
	second, _ := browserSessionRecord("second", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.CreateLocalSession(ctx, second); err != session.ErrBrowserSessionConflict || principal.SessionID != "" {
		t.Fatalf("active browser session was replaced: %v", err)
	}
	var count int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", second.SessionID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("denied session was stored: count=%d err=%v", count, err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.start", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditSucceeded || events[1].Outcome != AuditDenied {
		t.Fatalf("browser session conflict was not audited: count=%d err=%v", len(events), err)
	}
	if strings.Contains(fmt.Sprint(events), marker) {
		t.Fatal("browser marker appeared in audit events")
	}
	if _, err := f.db.UseSession(ctx, first.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
		t.Fatalf("unbound resolver accepted a browser-bound session: %v", err)
	}
	if grants, err := f.db.ResolveSessionGrants(ctx, first.TokenHash, "", f.clock(), nil); err != session.ErrInvalidSession || len(grants) != 0 {
		t.Fatalf("unbound grant resolver accepted a browser-bound session: %v", err)
	}
	if _, err := f.db.db.Exec("INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', 'ADMIN')", first.AccountID); err != nil {
		t.Fatal(err)
	}
	tx, err := f.db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = authorizeLocalGlobalAdmin(ctx, tx, identity.LocalAdminActor{AccountID: first.AccountID, SessionID: first.SessionID}, f.clock())
	rollbackErr := tx.Rollback()
	if err != identity.ErrInvalidAdminSession || rollbackErr != nil {
		t.Fatalf("unbound admin authorization accepted a browser-bound session: %v (rollback: %v)", err, rollbackErr)
	}
	if err := f.db.RevokeBoundSession(ctx, first.TokenHash, binding, "browser-revocation-event", f.clock()); err != nil {
		t.Fatal(err)
	}
	replacement, _ := browserSessionRecord("replacement", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
		t.Fatalf("revoked session still occupied its browser: %v", err)
	}
	var current string
	var retained []byte
	if err := f.db.db.QueryRow("SELECT session_id FROM console_browser_sessions WHERE browser_hash = ?", binding[:]).Scan(&current); err != nil || current != replacement.SessionID {
		t.Fatalf("browser binding was not transferred after revocation: %v", err)
	}
	if err := f.db.db.QueryRow("SELECT browser_binding_hash FROM console_sessions WHERE id = ?", first.SessionID).Scan(&retained); err != nil || !bytes.Equal(retained, binding[:]) {
		t.Fatal("replaced session lost its historical browser binding")
	}
}

func TestBrowserSessionExpiredAndDisabledBindingsCanBeReplaced(t *testing.T) {
	for _, mode := range []string{"idle", "absolute", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			first, _ := browserSessionRecord("inactive-first", "browser-local-a", binding, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "idle":
				f.current.Store(first.IdleExpiresAt.Add(-time.Nanosecond).UnixNano())
				early, _ := browserSessionRecord("idle-early", "browser-local-b", binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, early); err != session.ErrBrowserSessionConflict {
					t.Fatal("browser session became replaceable before idle expiry")
				}
				f.current.Store(first.IdleExpiresAt.UnixNano())
			case "absolute":
				if _, err := f.db.db.Exec("UPDATE console_sessions SET idle_expires_at_ns = absolute_expires_at_ns WHERE id = ?", first.SessionID); err != nil {
					t.Fatal(err)
				}
				f.current.Store(first.AbsoluteExpiresAt.Add(-time.Nanosecond).UnixNano())
				early, _ := browserSessionRecord("absolute-early", "browser-local-b", binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, early); err != session.ErrBrowserSessionConflict {
					t.Fatal("browser session became replaceable before absolute expiry")
				}
				f.current.Store(first.AbsoluteExpiresAt.UnixNano())
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", first.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			second, _ := browserSessionRecord("inactive-second", "browser-local-b", binding, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, second); err != nil {
				t.Fatalf("definitely inactive session blocked its browser: %v", err)
			}
			if mode == "disabled" {
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", first.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			var current string
			if err := f.db.db.QueryRow("SELECT session_id FROM console_browser_sessions WHERE browser_hash = ?", binding[:]).Scan(&current); err != nil || current != second.SessionID {
				t.Fatal("browser did not retain the replacement binding")
			}
			if _, err := f.db.UseSession(ctx, first.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
				t.Fatal("previous browser-bound session revived after replacement")
			}
		})
	}
}

func TestBrowserSessionQueuedAndCommittedLifetime(t *testing.T) {
	for _, stage := range []int{0, 3, 4} {
		t.Run(fmt.Sprintf("stage=%d", stage), func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, _ := browserSessionRecord("lifetime", "browser-local-a", binding, f.clock())
			var principal session.Principal
			var err error
			if stage == 0 {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(record.IdleExpiresAt.UnixNano())
				}, func() error {
					principal, err = f.db.CreateLocalSession(ctx, record)
					return err
				})
				if err != session.ErrInvalidSession {
					t.Fatalf("queued expired browser session was not rejected: %v", err)
				}
			} else {
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= stage {
						f.current.Store(record.IdleExpiresAt.UnixNano())
					}
					return f.clock()
				}
				principal, err = f.db.CreateLocalSession(ctx, record)
				if !errors.Is(err, session.ErrBrowserSessionUnknown) || errors.Is(err, session.ErrInvalidSession) {
					t.Fatalf("expired in-flight browser issuance did not report uncertainty: %v", err)
				}
			}
			if principal.SessionID != "" {
				t.Fatal("expired browser issuance returned a session")
			}
			var stored int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", record.SessionID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if (stage == 4 && stored != 1) || (stage != 4 && stored != 0) {
				t.Fatalf("unexpected stored session after expiry: stage=%d count=%d", stage, stored)
			}
			if stage == 0 {
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.start", Limit: 10})
				if err != nil || len(events) != 1 || events[0].Outcome != AuditValidationRejected {
					t.Fatal("queued expiry was not audited")
				}
			}
		})
	}
}

func TestBrowserSessionRejectsBindingEqualToSessionCredentials(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	for _, mode := range []string{"session", "csrf"} {
		t.Run(mode, func(t *testing.T) {
			record, _ := browserSessionRecord("collision-"+mode, "browser-local-a", binding, f.clock())
			if mode == "session" {
				record.BrowserBindingHash = record.TokenHash
			} else {
				record.BrowserBindingHash = record.CSRFHash
			}
			if principal, err := f.db.CreateLocalSession(context.Background(), record); err != session.ErrInvalidSessionConfig || principal.SessionID != "" {
				t.Fatal("browser binding matched a session or CSRF credential")
			}
		})
	}
}

func TestBrowserSessionRejectsBackwardsClock(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	record, _ := browserSessionRecord("backwards-clock", "browser-local-a", binding, f.clock())
	f.current.Store(record.CreatedAt.Add(-time.Nanosecond).UnixNano())
	principal, err := f.db.CreateLocalSession(context.Background(), record)
	if !errors.Is(err, ErrInvalidAuditEvent) || principal.SessionID != "" {
		t.Fatal("browser session creation accepted a backwards clock")
	}
	var count int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", record.SessionID).Scan(&count); err != nil || count != 0 {
		t.Fatal("backwards-clock session was persisted")
	}
}

func TestBrowserSessionCognitoLifetimeAndClock(t *testing.T) {
	for _, mode := range []string{"expiry", "backwards"} {
		for _, stage := range []int{3, 7, 9} {
			t.Run(fmt.Sprintf("%s/stage=%d", mode, stage), func(t *testing.T) {
				f, binding, _ := newBrowserSessionFixture(t)
				record, _, _ := browserCognitoRecord(t, f, "cognito-lifetime", binding)
				record.Session.IdleExpiresAt = record.Session.CreatedAt.Add(time.Minute)
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= stage {
						if mode == "expiry" {
							f.current.Store(record.Session.IdleExpiresAt.UnixNano())
						} else {
							f.current.Store(record.Session.CreatedAt.Add(-time.Nanosecond).UnixNano())
						}
					}
					return f.clock()
				}
				principal, err := f.db.CreateCognitoSession(context.Background(), record)
				if principal.SessionID != "" || err == nil || errors.Is(err, session.ErrInvalidSession) {
					t.Fatal("invalid browser-bound Cognito session produced credentials")
				}
				if stage == 3 && mode == "expiry" && !errors.Is(err, session.ErrCognitoSessionUnavailable) {
					t.Fatalf("queued Cognito expiry was not denied: %v", err)
				}
				if mode == "backwards" && !errors.Is(err, ErrInvalidAuditEvent) {
					t.Fatalf("backwards clock was not reported: %v", err)
				}
				if stage > 3 && !errors.Is(err, session.ErrBrowserSessionUnknown) {
					t.Fatalf("uncertain browser-session outcome was not reported: %v", err)
				}
				var count int
				if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", record.Session.SessionID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if (stage < 9 && count != 0) || (stage == 9 && count != 1) {
					t.Fatalf("unexpected Cognito session persistence at stage %d: %d", stage, count)
				}
			})
		}
	}
}

func TestBrowserSessionCognitoAndLocalIssuanceConflict(t *testing.T) {
	for _, firstSource := range []string{"local", "cognito"} {
		t.Run(firstSource, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			local, _ := browserSessionRecord("local-"+firstSource, "browser-local-a", binding, f.clock())
			remote, token, protector := browserCognitoRecord(t, f, "cognito-"+firstSource, binding)
			if firstSource == "local" {
				if _, err := f.db.CreateLocalSession(ctx, local); err != nil {
					t.Fatal(err)
				}
				if principal, err := f.db.CreateCognitoSession(ctx, remote); err != session.ErrBrowserSessionConflict || principal.SessionID != "" {
					t.Fatalf("Cognito session replaced a local session: %v", err)
				}
				var identities, credentials int
				if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_identities").Scan(&identities); err != nil {
					t.Fatal(err)
				}
				if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials").Scan(&credentials); err != nil || identities != 0 || credentials != 0 {
					t.Fatal("denied Cognito session stored an identity or credential")
				}
			} else {
				if _, err := f.db.CreateCognitoSession(ctx, remote); err != nil {
					t.Fatal(err)
				}
				if principal, err := f.db.CreateLocalSession(ctx, local); err != session.ErrBrowserSessionConflict || principal.SessionID != "" {
					t.Fatalf("local session replaced a Cognito session: %v", err)
				}
				manager, err := session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock,
					session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, protector)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := manager.Resolve(ctx, token); err != session.ErrInvalidSession {
					t.Fatalf("unbound manager accepted a bound Cognito session: %v", err)
				}
				if _, err := manager.ResolveForLogout(ctx, token); err != session.ErrInvalidSession {
					t.Fatalf("unbound logout accepted a bound Cognito session: %v", err)
				}
				if grants, err := f.db.ResolveSessionGrants(ctx, remote.Session.TokenHash, "cluster-a", f.clock(), protector); err != session.ErrInvalidSession || len(grants) != 0 {
					t.Fatal("unbound grant resolver accepted a bound Cognito session")
				}
				if _, err := f.db.ReserveCognitoRefresh(ctx, cognito.RefreshReservationRequest{
					OperationID: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{13}, 16)), TokenHash: remote.Session.TokenHash,
					Protector: protector, OccurredAt: f.clock(),
				}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("unbound refresh reservation accepted a bound session: %v", err)
				}
				refreshEvents, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
				if err != nil || len(refreshEvents) != 1 || refreshEvents[0].Outcome != AuditDenied {
					t.Fatal("bound refresh denial was not audited")
				}
				f.current.Store(f.clock().Add(5 * time.Minute).UnixNano())
				if _, err := manager.Resolve(ctx, token); err != session.ErrInvalidSession || f.refreshCalls.Load() != 0 {
					t.Fatal("bound Cognito session triggered unbound provider refresh")
				}
			}
			if f.tokenCalls.Load() != 1 {
				t.Fatal("browser-session conflict retried the provider exchange")
			}
		})
	}
}

func TestBrowserSessionCognitoConflictAuditFailure(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	local, _ := browserSessionRecord("audit-local", "browser-local-a", binding, f.clock())
	remote, _, _ := browserCognitoRecord(t, f, "audit-cognito", binding)
	if _, err := f.db.CreateLocalSession(ctx, local); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`CREATE TRIGGER reject_browser_cognito_denial BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.session' AND NEW.outcome = 'denied'
BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`); err != nil {
		t.Fatal(err)
	}
	principal, err := f.db.CreateCognitoSession(ctx, remote)
	if !errors.Is(err, session.ErrCognitoSessionAudit) || err == session.ErrBrowserSessionConflict || principal.SessionID != "" {
		t.Fatalf("Cognito conflict audit failure was hidden: %v", err)
	}
	var count int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", remote.Session.SessionID).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed Cognito denial left a session")
	}
}

func TestBrowserSessionCognitoCommitFailureIsUnknown(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	record, _, _ := browserCognitoRecord(t, f, "cognito-commit", binding)
	if _, err := f.db.db.Exec(`CREATE TABLE browser_cognito_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_browser_cognito_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.session' AND NEW.outcome = 'succeeded'
BEGIN INSERT INTO browser_cognito_commit_failure (identity_id) VALUES ('missing-identity'); END;`); err != nil {
		t.Fatal(err)
	}
	principal, err := f.db.CreateCognitoSession(context.Background(), record)
	if !errors.Is(err, session.ErrBrowserSessionUnknown) || !errors.Is(err, session.ErrCognitoSessionUnknown) ||
		errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" {
		t.Fatalf("Cognito commit failure was not treated as an unknown browser outcome: %v", err)
	}
	var count int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", record.Session.SessionID).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed Cognito commit retained a session")
	}
}

func TestBrowserSessionConcurrentLocalAndCognitoIssuance(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	local, token := browserSessionRecord("concurrent-local", "browser-local-a", binding, f.clock())
	remote, _, _ := browserCognitoRecord(t, f, "concurrent-cognito", binding)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		_, err := f.db.CreateLocalSession(ctx, local)
		results <- err
	}()
	go func() {
		defer workers.Done()
		<-start
		_, err := f.db.CreateCognitoSession(ctx, remote)
		results <- err
	}()
	close(start)
	workers.Wait()
	close(results)
	succeeded, denied := 0, 0
	for err := range results {
		switch err {
		case nil:
			succeeded++
		case session.ErrBrowserSessionConflict:
			denied++
		default:
			t.Fatalf("concurrent session issuance failed unexpectedly: %v", err)
		}
	}
	if succeeded != 1 || denied != 1 {
		t.Fatalf("concurrent issuance: successes=%d denials=%d", succeeded, denied)
	}
	var bindings, sessions int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_browser_sessions").Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ?", binding[:]).Scan(&sessions); err != nil || bindings != 1 || sessions != 1 {
		t.Fatalf("concurrent issuance persisted multiple browser sessions: bindings=%d sessions=%d err=%v", bindings, sessions, err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil || strings.Contains(fmt.Sprint(events), marker) || strings.Contains(fmt.Sprint(events), token) {
		t.Fatal("browser or session token appeared in audit output")
	}
	for _, path := range []string{f.path, f.path + "-wal"} {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(marker)) || bytes.Contains(data, []byte(token)) {
			t.Fatal("browser or session token was stored in plaintext")
		}
	}
}

func TestBrowserSessionConcurrentCognitoCallbacks(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	first, _, _ := browserCognitoRecord(t, f, "callback-first", binding)
	second, _, _ := browserCognitoRecord(t, f, "callback-second", binding)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, record := range []session.CognitoRecord{first, second} {
		workers.Add(1)
		go func(record session.CognitoRecord) {
			defer workers.Done()
			<-start
			_, err := f.db.CreateCognitoSession(ctx, record)
			results <- err
		}(record)
	}
	close(start)
	workers.Wait()
	close(results)
	succeeded, denied := 0, 0
	for err := range results {
		switch err {
		case nil:
			succeeded++
		case session.ErrBrowserSessionConflict:
			denied++
		default:
			t.Fatalf("concurrent Cognito callback failed unexpectedly: %v", err)
		}
	}
	if succeeded != 1 || denied != 1 || f.tokenCalls.Load() != 2 {
		t.Fatal("competing Cognito callbacks did not produce exactly one bound session")
	}
	var sessions int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ?", binding[:]).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("concurrent callbacks stored multiple sessions: count=%d err=%v", sessions, err)
	}
}

func TestBrowserSessionConcurrentRevocationAndIssuance(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	first, _ := browserSessionRecord("revoke-first", "browser-local-a", binding, f.clock())
	second, _ := browserSessionRecord("revoke-second", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	revoked, issued := make(chan error, 1), make(chan error, 1)
	go func() {
		<-start
		revoked <- f.db.RevokeBoundSession(ctx, first.TokenHash, binding, "browser-concurrent-revocation", f.clock())
	}()
	go func() {
		<-start
		_, err := f.db.CreateLocalSession(ctx, second)
		issued <- err
	}()
	close(start)
	if err := <-revoked; err != nil {
		t.Fatalf("concurrent revocation failed: %v", err)
	}
	if err := <-issued; err != nil && err != session.ErrBrowserSessionConflict {
		t.Fatalf("concurrent issuance failed unexpectedly: %v", err)
	}
	var active int
	if err := f.db.db.QueryRow(`SELECT COUNT(*) FROM console_sessions
WHERE browser_binding_hash = ? AND revoked_at_ns IS NULL`, binding[:]).Scan(&active); err != nil || active > 1 {
		t.Fatalf("revocation race left multiple active browser sessions: count=%d err=%v", active, err)
	}
}

func TestBrowserSessionMigrationRestartAndUnboundIsolation(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	legacy, _ := browserSessionRecord("legacy", "browser-local-a", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`DROP TABLE console_browser_sessions;
ALTER TABLE console_sessions DROP COLUMN browser_binding_hash;
DROP TABLE policy_state; ALTER TABLE audit_events DROP COLUMN before_state;
ALTER TABLE audit_events DROP COLUMN after_state; PRAGMA user_version = 9`); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	var version, count int
	if err := f.db.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 11 {
		t.Fatalf("version-nine database did not migrate: version=%d err=%v", version, err)
	}
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_browser_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatal("migration fabricated browser bindings")
	}
	if _, err := f.db.UseSession(ctx, legacy.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != nil {
		t.Fatalf("legacy unbound session stopped working: %v", err)
	}
	bound, _ := browserSessionRecord("persisted", "browser-local-a", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, bound); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	competitor, _ := browserSessionRecord("after-restart", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, competitor); err != session.ErrBrowserSessionConflict {
		t.Fatalf("browser session reservation was lost on restart: %v", err)
	}
	if _, err := f.db.UseSession(ctx, legacy.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != nil {
		t.Fatalf("legacy session was affected by bound session: %v", err)
	}
}

func TestBrowserSessionAuditCommitAndCorruptionFailures(t *testing.T) {
	for _, mode := range []string{"denial audit", "denial commit", "binding insert", "success commit", "corrupt binding", "missing table"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			first, _ := browserSessionRecord("failure-first", "browser-local-a", binding, f.clock())
			second, _ := browserSessionRecord("failure-second", "browser-local-b", binding, f.clock())
			if mode == "denial audit" || mode == "denial commit" || mode == "corrupt binding" {
				if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
					t.Fatal(err)
				}
			}
			var sql string
			switch mode {
			case "denial audit":
				sql = `CREATE TRIGGER reject_browser_denial BEFORE INSERT ON audit_events
WHEN NEW.event_id = 'browser-session-event-failure-second'
BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
			case "denial commit", "success commit":
				sql = `CREATE TABLE browser_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_browser_commit AFTER INSERT ON audit_events
WHEN NEW.event_id = 'browser-session-event-failure-second'
BEGIN INSERT INTO browser_commit_failure (identity_id) VALUES ('missing-identity'); END;`
			case "binding insert":
				sql = `CREATE TRIGGER reject_browser_binding BEFORE INSERT ON console_browser_sessions
BEGIN SELECT RAISE(FAIL, 'synthetic binding failure'); END;`
			case "corrupt binding":
				sql = `UPDATE console_sessions SET browser_binding_hash = zeroblob(32) WHERE id = 'browser-session-failure-first'`
			case "missing table":
				sql = `DROP TABLE console_browser_sessions`
			}
			if _, err := f.db.db.Exec(sql); err != nil {
				t.Fatal(err)
			}
			principal, err := f.db.CreateLocalSession(ctx, second)
			if err == nil || err == session.ErrBrowserSessionConflict || principal.SessionID != "" {
				t.Fatalf("operational failure returned success or a definite conflict: %v", err)
			}
			var stored int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE id = ?", second.SessionID).Scan(&stored); err != nil || stored != 0 {
				t.Fatalf("failed browser issuance persisted a session: count=%d err=%v", stored, err)
			}
			if mode == "missing table" {
				unbound, _ := browserSessionRecord("unbound-without-mapping", "browser-local-b", [32]byte{}, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, unbound); err != nil {
					t.Fatalf("unbound local session depended on browser mapping: %v", err)
				}
				if _, err := f.db.UseSession(ctx, unbound.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != nil {
					t.Fatalf("unbound local session use depended on browser mapping: %v", err)
				}
			}
		})
	}
}
