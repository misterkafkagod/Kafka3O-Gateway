package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerLogoutWinsDuringRefresh(t *testing.T) {
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
	principal, err := f.sessions.ResolveBoundForLogout(context.Background(), f.credentials.Token, f.binding)
	if err != nil || principal.SessionID != f.credentials.Principal.SessionID || principal.CSRFHash != f.record.Session.CSRFHash {
		t.Fatalf("pending bound refresh blocked local logout: %v", err)
	}
	if err := f.sessions.RevokeBound(context.Background(), f.credentials.Token, f.binding); err != nil {
		t.Fatalf("bound local logout failed during refresh: %v", err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, session.ErrCognitoRefreshUnavailable) {
			t.Fatalf("in-flight refresh restored the logged-out session: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight bound refresh did not finish")
	}
	if _, err := f.sessions.ResolveBoundForLogout(context.Background(), f.credentials.Token, f.binding); err != session.ErrInvalidSession ||
		f.refreshCalls.Load() != 1 {
		t.Fatalf("logout allowed refresh replay or session revival: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("in-flight logout retained the provider credential: %v", err)
	}
}

func TestBoundSessionManagerLogoutCannotRevokeReplacement(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	ctx := context.Background()
	if principal, err := f.sessions.ResolveBoundForLogout(ctx, f.credentials.Token, f.binding); err != nil ||
		principal.SessionID != f.credentials.Principal.SessionID {
		t.Fatalf("original bound logout lookup failed: %v", err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	replacement, replacementToken := browserSessionRecord("logout-replacement", "browser-local-b", f.binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	if err := f.sessions.RevokeBound(ctx, f.credentials.Token, f.binding); err != session.ErrInvalidSession {
		t.Fatalf("old bound logout revoked a replacement: %v", err)
	}
	if principal, err := f.sessions.ResolveBoundForLogout(ctx, replacementToken, f.binding); err != nil ||
		principal.SessionID != replacement.SessionID || f.refreshCalls.Load() != 0 {
		t.Fatalf("replacement was affected by old bound logout: %v", err)
	}
}
