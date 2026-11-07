package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerLogoutLocalAndUnboundIsolation(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	bound, token := browserSessionRecord("manager-logout", "browser-local-a", binding, f.clock())
	unbound, unboundToken := browserSessionRecord("manager-logout-unbound", "browser-local-b", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, bound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CreateLocalSession(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := manager.ResolveBoundForLogout(ctx, token, binding)
	if err != nil || principal.SessionID != bound.SessionID || principal.CSRFHash != bound.CSRFHash {
		t.Fatalf("bound local logout could not validate the current session: %v", err)
	}
	wrong := sha256.Sum256([]byte("other logout browser"))
	for _, invalid := range [][32]byte{{}, wrong} {
		if principal, err := manager.ResolveBoundForLogout(ctx, token, invalid); err != session.ErrInvalidSession || principal.SessionID != "" {
			t.Fatalf("bound logout accepted a wrong or missing marker: %v", err)
		}
		if err := manager.RevokeBound(ctx, token, invalid); err != session.ErrInvalidSession {
			t.Fatalf("bound revocation accepted a wrong or missing marker: %v", err)
		}
	}
	if _, err := manager.ResolveForLogout(ctx, token); err != session.ErrInvalidSession {
		t.Fatalf("unbound logout accepted a bound session: %v", err)
	}
	if err := manager.Revoke(ctx, token); err != session.ErrInvalidSession {
		t.Fatalf("unbound revocation accepted a bound session: %v", err)
	}
	if _, err := manager.ResolveBoundForLogout(ctx, unboundToken, binding); err != session.ErrInvalidSession {
		t.Fatalf("bound logout accepted an unbound session: %v", err)
	}
	if err := manager.RevokeBound(ctx, unboundToken, binding); err != session.ErrInvalidSession {
		t.Fatalf("bound revocation accepted an unbound session: %v", err)
	}
	f.reopen(t)
	manager, err = session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RevokeBound(ctx, token, binding); err != nil {
		t.Fatalf("current bound local logout failed after restart: %v", err)
	}
	if _, err := manager.ResolveBoundForLogout(ctx, token, binding); err != session.ErrInvalidSession {
		t.Fatalf("revoked bound local session remained available: %v", err)
	}
	if principal, err := manager.ResolveForLogout(ctx, unboundToken); err != nil || principal.SessionID != unbound.SessionID {
		t.Fatalf("bound logout affected an unbound session: %v", err)
	}
	if err := manager.Revoke(ctx, unboundToken); err != nil {
		t.Fatalf("unbound logout changed behavior: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditSucceeded || events[1].Outcome != AuditSucceeded {
		t.Fatalf("local logout was not audited: count=%d err=%v", len(events), err)
	}
	for _, secret := range []string{marker, token, unboundToken, base64.RawURLEncoding.EncodeToString(binding[:])} {
		if strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("bound logout audit exposed a credential")
		}
	}
}

func TestBoundSessionManagerLogoutCognitoWithoutProvider(t *testing.T) {
	for _, mode := range []string{"stale", "disabled", "expired", "missing-credential", "corrupt-credential"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundRefreshingSessionFixture(t, "refresh outage")
			ctx := context.Background()
			switch mode {
			case "stale":
				f.current.Store(f.freshUntil)
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.record.Session.AbsoluteExpiresAt.Add(time.Second).UnixNano())
			case "missing-credential":
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			case "corrupt-credential":
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?",
					bytes.Repeat([]byte{1}, 32), f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			principal, err := f.sessions.ResolveBoundForLogout(ctx, f.credentials.Token, f.binding)
			if err != nil || principal.SessionID != f.credentials.Principal.SessionID || principal.Source != session.SourceCognito ||
				principal.CSRFHash != f.record.Session.CSRFHash || f.refreshCalls.Load() != 0 {
				t.Fatalf("unavailable bound Cognito session could not be locally logged out: %v", err)
			}
			if err := f.sessions.RevokeBound(ctx, f.credentials.Token, f.binding); err != nil {
				t.Fatalf("bound Cognito revocation failed: %v", err)
			}
			var credentials int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
				f.credentials.Principal.SessionID).Scan(&credentials); err != nil || credentials != 0 {
				t.Fatalf("bound Cognito logout retained a provider credential: %v", err)
			}
			if _, err := f.sessions.ResolveBoundForLogout(ctx, f.credentials.Token, f.binding); err != session.ErrInvalidSession || f.refreshCalls.Load() != 0 {
				t.Fatalf("revoked bound session remained available or contacted provider: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditSucceeded || events[0].Source != AuditSourceCognito {
				t.Fatalf("bound Cognito logout was not audited: count=%d err=%v", len(events), err)
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
				f.provider.Issuer, "opaque|synthetic-subject", fixtureRefresh, fixtureRotated} {
				if strings.Contains(fmt.Sprint(principal, events), secret) {
					t.Fatal("bound Cognito logout exposed sensitive material")
				}
			}
		})
	}
}

func TestBoundSessionManagerLogoutReportsAuditFailure(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	ctx := context.Background()
	if _, err := f.db.db.Exec(`CREATE TRIGGER reject_bound_manager_logout BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.logout'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	err := f.sessions.RevokeBound(ctx, f.credentials.Token, f.binding)
	if err == nil || errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("logout audit failure was hidden as invalidity: %v", err)
	}
	if principal, err := f.sessions.ResolveBoundForLogout(ctx, f.credentials.Token, f.binding); err != nil || principal.SessionID != f.credentials.Principal.SessionID {
		t.Fatalf("failed audit did not roll back logout: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("failed logout audit removed the provider credential: %v", err)
	}
	if f.refreshCalls.Load() != 0 {
		t.Fatal("failed bound logout contacted the provider")
	}
}

func TestBoundSessionManagerLogoutReportsMissingBindingStorage(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	ctx := context.Background()
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.sessions.ResolveBoundForLogout(ctx, f.credentials.Token, f.binding); err == nil ||
		errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" {
		t.Fatalf("bound logout hid unavailable binding storage: %v", err)
	}
	if err := f.sessions.RevokeBound(ctx, f.credentials.Token, f.binding); err == nil || errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("bound revocation hid unavailable binding storage: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 1 || f.refreshCalls.Load() != 0 {
		t.Fatalf("binding storage failure changed credentials or contacted provider: %v", err)
	}
}
