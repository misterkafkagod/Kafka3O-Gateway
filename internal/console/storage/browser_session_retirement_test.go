package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type browserRetirementFixture struct {
	fixture   *cognitoCodeFixture
	hash      [32]byte
	marker    string
	old       session.Record
	oldToken  string
	protector *cognito.RefreshTokenProtector
}

func newBrowserRetirementFixture(t *testing.T, source string) browserRetirementFixture {
	t.Helper()
	f, hash, marker := newBrowserSessionFixture(t)
	result := browserRetirementFixture{fixture: f, hash: hash, marker: marker}
	if source == "local" {
		result.old, result.oldToken = browserSessionRecord("retire-old", "browser-local-a", hash, f.clock())
		if _, err := f.db.CreateLocalSession(context.Background(), result.old); err != nil {
			t.Fatal(err)
		}
	} else {
		record, token, protector := browserCognitoRecord(t, f, "retire-old", hash)
		principal, err := f.db.CreateCognitoSession(context.Background(), record)
		if err != nil {
			t.Fatal(err)
		}
		result.old, result.oldToken, result.protector = record.Session, token, protector
		result.old.AccountID = principal.AccountID
	}
	return result
}

type browserRetirementReplacement struct {
	local       session.Record
	cognito     session.CognitoRecord
	token       string
	protector   *cognito.RefreshTokenProtector
	operationID string
}

func newBrowserRetirementReplacement(t *testing.T, f browserRetirementFixture, source, name string) browserRetirementReplacement {
	t.Helper()
	if source == "local" {
		record, token := browserSessionRecord(name, "browser-local-b", f.hash, f.fixture.clock())
		return browserRetirementReplacement{local: record, token: token, operationID: record.EventID}
	}
	record, token, protector := browserCognitoRecord(t, f.fixture, name, f.hash)
	details, err := record.Exchange.BoundDetails(protector, f.fixture.clock(), f.hash)
	if err != nil {
		t.Fatal(err)
	}
	return browserRetirementReplacement{cognito: record, token: token, protector: protector, operationID: details.OperationID}
}

func (r browserRetirementReplacement) create(ctx context.Context, db *Database) (session.Principal, error) {
	if r.local.SessionID != "" {
		return db.CreateLocalSession(ctx, r.local)
	}
	return db.CreateCognitoSession(ctx, r.cognito)
}

func TestBrowserSessionRetirementReplacesInactiveSessions(t *testing.T) {
	for _, test := range []struct {
		oldSource, newSource, inactive string
	}{
		{"cognito", "local", "disabled"},
		{"cognito", "cognito", "disabled"},
		{"cognito", "cognito", "idle"},
		{"cognito", "local", "absolute"},
		{"cognito", "local", "revoked"},
		{"local", "cognito", "disabled"},
		{"local", "local", "idle"},
		{"local", "local", "revoked"},
	} {
		t.Run(test.oldSource+"/"+test.newSource+"/"+test.inactive, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, test.oldSource)
			ctx := context.Background()
			switch test.inactive {
			case "disabled":
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
			case "idle":
				f.fixture.current.Store(f.old.IdleExpiresAt.UnixNano())
			case "absolute":
				f.fixture.current.Store(f.old.AbsoluteExpiresAt.UnixNano())
			case "revoked":
				if err := f.fixture.db.RevokeBoundSession(ctx, f.old.TokenHash, f.hash, "retire-existing-revocation", f.fixture.clock()); err != nil {
					t.Fatal(err)
				}
			}
			if test.oldSource == "cognito" && test.newSource == "cognito" && test.inactive == "disabled" {
				f.fixture.mu.Lock()
				f.fixture.mode = "other identity"
				f.fixture.mu.Unlock()
			}
			replacement := newBrowserRetirementReplacement(t, f, test.newSource, "retire-new")
			principal, err := replacement.create(ctx, f.fixture.db)
			if err != nil || principal.SessionID == "" {
				t.Fatalf("inactive session could not be replaced: %v", err)
			}
			var revoked sql.NullInt64
			var credentials int
			var current string
			if err := f.fixture.db.db.QueryRow(`SELECT revoked_at_ns,
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?)
FROM console_sessions WHERE id = ?`, f.old.SessionID, f.old.SessionID).Scan(&revoked, &credentials); err != nil || !revoked.Valid || credentials != 0 {
				t.Fatalf("replaced session retained credentials or was not revoked: %v", err)
			}
			if err := f.fixture.db.db.QueryRow("SELECT session_id FROM console_browser_sessions WHERE browser_hash = ?", f.hash[:]).Scan(&current); err != nil || current != principal.SessionID {
				t.Fatalf("replacement binding was not committed: %v", err)
			}
			events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.retire", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditSucceeded || events[0].Source != AuditSourceService ||
				events[0].ResourceID != f.old.SessionID || events[0].OperationID != replacement.operationID {
				t.Fatalf("replacement retirement was not audited: count=%d err=%v", len(events), err)
			}
			serialized, err := json.Marshal(events)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.hash[:]), f.oldToken, replacement.token, fixtureRefresh} {
				if bytes.Contains(serialized, []byte(secret)) || strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("retirement audit exposed a marker or credential")
				}
			}
			if test.inactive == "disabled" {
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			f.fixture.reopen(t)
			if _, err := f.fixture.db.UseBoundSession(ctx, f.old.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
				t.Fatalf("replaced session revived after reopening storage: %v", err)
			}
			var tokenHash [32]byte
			if test.newSource == "local" {
				tokenHash = replacement.local.TokenHash
			} else {
				tokenHash = replacement.cognito.Session.TokenHash
			}
			if restored, err := f.fixture.db.UseBoundSession(ctx, tokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, replacement.protector); err != nil || restored.SessionID != principal.SessionID {
				t.Fatalf("replacement session failed after reopening storage: %v", err)
			}
			if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", f.old.SessionID).Scan(&credentials); err != nil || credentials != 0 {
				t.Fatal("old provider credentials reappeared after reopening storage")
			}
		})
	}
}

func TestBrowserSessionRetirementPreservesActiveConflict(t *testing.T) {
	for _, newSource := range []string{"local", "cognito"} {
		t.Run(newSource, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "cognito")
			replacement := newBrowserRetirementReplacement(t, f, newSource, "active-conflict")
			if principal, err := replacement.create(context.Background(), f.fixture.db); err != session.ErrBrowserSessionConflict || principal.SessionID != "" {
				t.Fatalf("active session was replaced: %v", err)
			}
			var revoked sql.NullInt64
			var count int
			if err := f.fixture.db.db.QueryRow(`SELECT revoked_at_ns,
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?)
FROM console_sessions WHERE id = ?`, f.old.SessionID, f.old.SessionID).Scan(&revoked, &count); err != nil || revoked.Valid || count != 1 {
				t.Fatal("active conflict revoked a session or removed its credentials")
			}
			events, err := f.fixture.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.session.retire", Limit: 10})
			if err != nil || len(events) != 0 {
				t.Fatalf("active conflict produced a retirement audit: count=%d err=%v", len(events), err)
			}
			if principal, err := f.fixture.db.UseBoundSession(context.Background(), f.old.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, f.protector); err != nil || principal.SessionID != f.old.SessionID {
				t.Fatalf("active conflict damaged the current session: %v", err)
			}
		})
	}
}

func TestBrowserSessionRetirementRollsBackOnFailure(t *testing.T) {
	for _, newSource := range []string{"local", "cognito"} {
		for _, mode := range []string{"revocation", "credential deletion", "ignored deletion", "retirement audit", "replacement insert", "replacement audit", "commit"} {
			t.Run(newSource+"/"+mode, func(t *testing.T) {
				f := newBrowserRetirementFixture(t, "cognito")
				ctx := context.Background()
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
				if newSource == "cognito" {
					f.fixture.mu.Lock()
					f.fixture.mode = "other identity"
					f.fixture.mu.Unlock()
				}
				replacement := newBrowserRetirementReplacement(t, f, newSource, "retire-failure")
				var script string
				switch mode {
				case "revocation":
					script = `CREATE TRIGGER reject_retirement_revoke BEFORE UPDATE OF revoked_at_ns ON console_sessions
BEGIN SELECT RAISE(FAIL, 'revocation unavailable'); END;`
				case "credential deletion":
					script = `CREATE TRIGGER reject_retirement_delete BEFORE DELETE ON cognito_session_credentials
BEGIN SELECT RAISE(FAIL, 'credential storage unavailable'); END;`
				case "ignored deletion":
					script = `CREATE TRIGGER ignore_retirement_delete BEFORE DELETE ON cognito_session_credentials
BEGIN SELECT RAISE(IGNORE); END;`
				case "retirement audit":
					script = `CREATE TRIGGER reject_retirement_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.retire' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
				case "replacement insert":
					script = `CREATE TRIGGER reject_replacement_insert BEFORE INSERT ON console_sessions
BEGIN SELECT RAISE(FAIL, 'session storage unavailable'); END;`
				case "replacement audit":
					script = `CREATE TRIGGER reject_replacement_audit BEFORE INSERT ON audit_events
WHEN NEW.outcome = 'succeeded' AND NEW.action IN ('console.session.start', 'console.cognito.session')
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
				case "commit":
					script = `CREATE TABLE retirement_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_retirement_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.session.retire'
BEGIN INSERT INTO retirement_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				}
				if _, err := f.fixture.db.db.Exec(script); err != nil {
					t.Fatal(err)
				}
				principal, err := replacement.create(ctx, f.fixture.db)
				if err == nil || err == session.ErrBrowserSessionConflict || principal.SessionID != "" {
					t.Fatalf("retirement failure returned a session or false conflict: %v", err)
				}
				if mode == "retirement audit" && !errors.Is(err, session.ErrBrowserSessionAudit) {
					t.Fatalf("retirement audit failure was hidden: %v", err)
				}
				if mode == "commit" && !errors.Is(err, session.ErrBrowserSessionUnknown) {
					t.Fatalf("retirement commit failure was not uncertain: %v", err)
				}
				f.fixture.reopen(t)
				var revoked sql.NullInt64
				var credentials, sessions int
				var current string
				if err := f.fixture.db.db.QueryRow(`SELECT revoked_at_ns,
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?)
FROM console_sessions WHERE id = ?`, f.old.SessionID, f.old.SessionID).Scan(&revoked, &credentials); err != nil || revoked.Valid || credentials != 1 {
					t.Fatalf("failed replacement changed the old session: %v", err)
				}
				if err := f.fixture.db.db.QueryRow("SELECT session_id FROM console_browser_sessions WHERE browser_hash = ?", f.hash[:]).Scan(&current); err != nil || current != f.old.SessionID {
					t.Fatal("failed replacement changed the current browser binding")
				}
				if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ?", f.hash[:]).Scan(&sessions); err != nil || sessions != 1 {
					t.Fatal("failed replacement left a partial new session")
				}
				events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.retire", Limit: 10})
				if err != nil || len(events) != 0 {
					t.Fatal("failed replacement committed a retirement audit")
				}
				if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.old.AccountID); err != nil {
					t.Fatal(err)
				}
				if principal, err := f.fixture.db.UseBoundSession(ctx, f.old.TokenHash, f.hash, f.fixture.clock, session.DefaultIdleLifetime, f.protector); err != nil || principal.SessionID != f.old.SessionID {
					t.Fatalf("rolled-back replacement damaged the old session: %v", err)
				}
			})
		}
	}
}

type retirementClockReader struct {
	fixture *cognitoCodeFixture
	advance time.Time
}

func (r retirementClockReader) Read(value []byte) (int, error) {
	count, err := rand.Read(value)
	r.fixture.current.Store(r.advance.UnixNano())
	return count, err
}

func TestBrowserSessionRetirementDoesNotCommitBeforeCognitoDenial(t *testing.T) {
	for _, mode := range []string{"encryption", "proof expiry", "clock regression"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrowserRetirementFixture(t, "cognito")
			ctx := context.Background()
			if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
				t.Fatal(err)
			}
			f.fixture.mu.Lock()
			f.fixture.mode = "other identity"
			f.fixture.mu.Unlock()
			replacement := newBrowserRetirementReplacement(t, f, "cognito", "retire-denial")
			var random io.Reader = io.LimitReader(rand.Reader, 0)
			if mode != "encryption" {
				details, err := replacement.cognito.Exchange.BoundDetails(replacement.protector, f.fixture.clock(), f.hash)
				if err != nil {
					t.Fatal(err)
				}
				target := details.ValidUntil
				if mode == "clock regression" {
					target = f.fixture.clock().Add(-time.Second)
				}
				random = retirementClockReader{fixture: f.fixture, advance: target}
			}
			protector, err := cognito.NewRefreshTokenProtector(f.fixture.manager, random, bytes.Repeat([]byte{9}, 32))
			if err != nil {
				t.Fatal(err)
			}
			replacement.cognito.Protector = protector
			principal, err := replacement.create(ctx, f.fixture.db)
			if principal.SessionID != "" || err == nil {
				t.Fatal("Cognito denial returned a replacement session")
			}
			if mode == "clock regression" {
				if !errors.Is(err, ErrInvalidAuditEvent) {
					t.Fatalf("Cognito clock regression was not surfaced: %v", err)
				}
			} else if !errors.Is(err, session.ErrCognitoSessionUnavailable) {
				t.Fatalf("Cognito validation failure was not surfaced: %v", err)
			}
			var revoked sql.NullInt64
			var credentials int
			if err := f.fixture.db.db.QueryRow(`SELECT revoked_at_ns,
  (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?)
FROM console_sessions WHERE id = ?`, f.old.SessionID, f.old.SessionID).Scan(&revoked, &credentials); err != nil || revoked.Valid || credentials != 1 {
				t.Fatal("Cognito denial committed retirement without a replacement")
			}
			events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.retire", Limit: 10})
			if err != nil || len(events) != 0 {
				t.Fatal("Cognito denial committed a retirement audit")
			}
		})
	}
}

func TestBrowserSessionRetirementSerializesConcurrentReplacements(t *testing.T) {
	f := newBrowserRetirementFixture(t, "cognito")
	ctx := context.Background()
	if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
		t.Fatal(err)
	}
	first := newBrowserRetirementReplacement(t, f, "local", "retire-race-a")
	second := newBrowserRetirementReplacement(t, f, "local", "retire-race-b")
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, replacement := range []browserRetirementReplacement{first, second} {
		wait.Add(1)
		go func(replacement browserRetirementReplacement) {
			defer wait.Done()
			_, err := replacement.create(ctx, f.fixture.db)
			results <- err
		}(replacement)
	}
	wait.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		switch err {
		case nil:
			success++
		case session.ErrBrowserSessionConflict:
			conflict++
		default:
			t.Fatalf("unexpected concurrent retirement result: %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("concurrent replacement did not serialize: success=%d conflict=%d", success, conflict)
	}
	var credentials, sessions int
	if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", f.old.SessionID).Scan(&credentials); err != nil || credentials != 0 {
		t.Fatal("concurrent replacement retained old credentials")
	}
	if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ? AND revoked_at_ns IS NULL", f.hash[:]).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatal("concurrent replacement created competing sessions")
	}
	events, err := f.fixture.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.retire", Limit: 10})
	if err != nil || len(events) != 1 || events[0].ResourceID != f.old.SessionID {
		t.Fatalf("concurrent retirement was not audited exactly once: count=%d err=%v", len(events), err)
	}
}

func TestBrowserSessionRetirementRejectsClockRegression(t *testing.T) {
	f := newBrowserRetirementFixture(t, "cognito")
	if _, err := f.fixture.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.old.AccountID); err != nil {
		t.Fatal(err)
	}
	f.fixture.current.Store(f.old.CreatedAt.Add(-time.Second).UnixNano())
	replacement := newBrowserRetirementReplacement(t, f, "local", "retire-clock")
	if principal, err := replacement.create(context.Background(), f.fixture.db); !errors.Is(err, ErrInvalidAuditEvent) || principal.SessionID != "" {
		t.Fatalf("clock regression retired a newer session: %v", err)
	}
	var credentials int
	if err := f.fixture.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", f.old.SessionID).Scan(&credentials); err != nil || credentials != 1 {
		t.Fatal("clock regression removed provider credentials")
	}
}
