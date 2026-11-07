package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundCognitoSessionRefreshFailsClosedOnStorageAndAuditErrors(t *testing.T) {
	for _, mode := range []string{"missing-binding-storage", "audit"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundRefreshingSessionFixture(t, "refresh rotated")
			f.current.Store(f.freshUntil)
			switch mode {
			case "missing-binding-storage":
				if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
					t.Fatal(err)
				}
			case "audit":
				if _, err := f.db.db.Exec(`CREATE TRIGGER reject_bound_refresh_success BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'succeeded'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
					t.Fatal(err)
				}
			}
			principal, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding)
			if err == nil || errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" {
				t.Fatalf("bound storage failure became a valid or invalid session: %v", err)
			}
			if mode == "missing-binding-storage" && f.refreshCalls.Load() != 0 {
				t.Fatal("missing bound storage reached the provider")
			}
			if mode == "audit" {
				if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || !errors.Is(err, cognito.ErrRefreshAudit) ||
					f.refreshCalls.Load() != 1 {
					t.Fatalf("bound completion audit failure was hidden: %v", err)
				}
				if _, err := f.db.db.Exec("DROP TRIGGER reject_bound_refresh_success"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
					errors.Is(err, session.ErrInvalidSession) || f.refreshCalls.Load() != 1 {
					t.Fatalf("bound audit failure was retried: %v", err)
				}
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
				fixtureRefresh, fixtureRotated, fixtureAccess, "opaque|synthetic-subject", f.provider.Issuer, "refreshed-group"} {
				if strings.Contains(fmt.Sprint(err), secret) {
					t.Fatal("bound refresh error exposed sensitive material")
				}
			}
		})
	}
}

func TestBoundCognitoSessionRefreshRejectsClockRegression(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	f.current.Store(f.record.Session.CreatedAt.Add(-time.Nanosecond).UnixNano())
	if principal, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding); err != session.ErrInvalidSessionConfig ||
		principal.SessionID != "" || f.refreshCalls.Load() != 0 {
		t.Fatalf("regressed bound clock granted access or dispatched refresh: %v", err)
	}
	f.current.Store(f.freshUntil)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f.mu.Lock()
	f.refreshEntered, f.refreshRelease = entered, release
	f.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bound provider refresh did not start")
	}
	f.current.Store(time.Unix(0, f.freshUntil).Add(-time.Minute).UnixNano())
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || errors.Is(err, session.ErrInvalidSession) {
			t.Fatalf("in-flight clock regression did not fail closed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("clock-regressed refresh did not finish")
	}
	f.current.Store(f.freshUntil)
	if _, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		f.refreshCalls.Load() != 1 {
		t.Fatalf("clock-regressed bound refresh was replayed: %v", err)
	}
}

func TestBoundCognitoSessionRefreshDoesNotReviveInFlightReplacement(t *testing.T) {
	for _, mode := range []string{"replaced", "revoked", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundRefreshingSessionFixture(t, "refresh rotated")
			f.current.Store(f.freshUntil)
			entered, release := make(chan struct{}, 1), make(chan struct{})
			f.mu.Lock()
			f.refreshEntered, f.refreshRelease = entered, release
			f.mu.Unlock()
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			type resolved struct {
				principal session.Principal
				err       error
			}
			done := make(chan resolved, 1)
			go func() {
				principal, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding)
				done <- resolved{principal: principal, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("bound provider refresh did not start")
			}
			var replacement session.Record
			var replacementToken string
			switch mode {
			case "replaced", "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
				if mode == "replaced" {
					replacement, replacementToken = browserSessionRecord("manager-replacement", "browser-local-b", f.binding, f.clock())
					if _, err := f.db.CreateLocalSession(context.Background(), replacement); err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
						t.Fatal(err)
					}
				}
			case "revoked":
				if err := f.db.RevokeBoundSession(context.Background(), f.record.Session.TokenHash, f.binding, "revoke-bound-manager", f.clock()); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			select {
			case result := <-done:
				if !errors.Is(result.err, session.ErrCognitoRefreshUnavailable) || result.principal.SessionID != "" {
					t.Fatalf("in-flight refresh restored an unavailable bound session: %v", result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("replaced bound refresh did not finish")
			}
			if principal, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding); err != session.ErrInvalidSession ||
				principal.SessionID != "" || f.refreshCalls.Load() != 1 {
				t.Fatalf("unavailable bound session revived or retried: %v", err)
			}
			if mode == "replaced" {
				if principal, err := f.sessions.ResolveBound(context.Background(), replacementToken, f.binding); err != nil ||
					principal.SessionID != replacement.SessionID {
					t.Fatalf("old bound refresh affected the replacement: %v", err)
				}
			}
		})
	}
}

func TestBoundCognitoSessionRefreshWrongMarkerCannotAffectOtherLocalBrowser(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	f.current.Store(f.freshUntil)
	localBinding := sha256.Sum256([]byte("separate local binding"))
	local, token := browserSessionRecord("independent-local", "browser-local-a", localBinding, f.clock())
	if _, err := f.db.CreateLocalSession(context.Background(), local); err != nil {
		t.Fatal(err)
	}
	if _, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, localBinding); err != session.ErrInvalidSession {
		t.Fatalf("different local browser accepted Cognito token: %v", err)
	}
	if principal, err := f.sessions.ResolveBound(context.Background(), token, localBinding); err != nil || principal.SessionID != local.SessionID ||
		f.refreshCalls.Load() != 0 {
		t.Fatalf("failed cross-browser lookup affected local session: %v", err)
	}
}
