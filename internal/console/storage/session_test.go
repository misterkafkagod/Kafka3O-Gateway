package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestLocalSessionsExpireAndPersist(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, now, 5*time.Minute, 20*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, manager)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := login.Execute(ctx, "admin", "initial passphrase", netip.MustParseAddr("192.0.2.1"))
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Principal.AccountID != account.ID || !credentials.Principal.MustChangePassword ||
		credentials.Token == "" || credentials.CSRFToken == "" || credentials.Token == credentials.CSRFToken {
		t.Fatal("local session lacks expected identity or independent credentials")
	}
	if err := session.CheckSessionAccess(credentials.Principal, session.AccessNormal); !errors.Is(err, session.ErrPasswordChangeRequired) {
		t.Fatalf("bootstrap account escaped password-change restriction: %v", err)
	}
	rawToken, err := base64.RawURLEncoding.DecodeString(credentials.Token)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(rawToken)
	var storedToken []byte
	if err := db.db.QueryRowContext(ctx, "SELECT token_hash FROM console_sessions WHERE id = ?", credentials.Principal.SessionID).Scan(&storedToken); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(storedToken, digest[:]) || bytes.Equal(storedToken, rawToken) {
		t.Fatal("session token was not stored exclusively as a digest")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager, err = session.NewManager(db, rand.Reader, now, 5*time.Minute, 20*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	start := at
	for _, minutes := range []int{4, 8, 12, 16, 19} {
		at = start.Add(time.Duration(minutes) * time.Minute)
		principal, err := manager.Resolve(ctx, credentials.Token)
		if err != nil || principal.AccountID != account.ID || principal.IdleExpiresAt.After(start.Add(20*time.Minute)) {
			t.Fatalf("session at minute %d: %v", minutes, err)
		}
	}
	at = start.Add(20 * time.Minute)
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("absolute expiry: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 30})
	if err != nil {
		t.Fatal(err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, value := range []string{credentials.Token, credentials.CSRFToken, "initial passphrase", secret} {
		if strings.Contains(serialized, value) {
			t.Fatal("session audit contains a credential")
		}
	}
}

func TestSessionIdleExpiryAndLogout(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	at := time.Now().UTC()
	now := func() time.Time { return at }
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", at); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, now, 5*time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	idle, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	logout, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if idle.Token == logout.Token || idle.Principal.SessionID == logout.Principal.SessionID {
		t.Fatal("session identifiers were reused")
	}
	if err := manager.Revoke(ctx, logout.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, logout.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("logged-out token accepted: %v", err)
	}
	if err := manager.Revoke(ctx, logout.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("duplicate logout: %v", err)
	}
	at = at.Add(5 * time.Minute)
	if _, err := manager.Resolve(ctx, idle.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("idle expiry boundary: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
	if err != nil || len(events) != 1 || events[0].ActorID != account.ID {
		t.Fatalf("logout audit count=%d err=%v", len(events), err)
	}
}

func TestPasswordChangeRevokesAllSessionsAndRejectsStaleLogin(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, manager)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.1")
	first, err := login.Execute(ctx, "admin", "initial passphrase", client)
	if err != nil {
		t.Fatal(err)
	}
	second, err := login.Execute(ctx, "admin", "initial passphrase", client)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := authenticator.Execute(ctx, "admin", "initial passphrase", client)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewChangeLocalPassword(authenticator, db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := change.Execute(ctx, account.ID, "admin", "initial passphrase", "replacement passphrase", client); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{first.Token, second.Token} {
		if _, err := manager.Resolve(ctx, token); !errors.Is(err, session.ErrInvalidSession) {
			t.Fatalf("session survived password change: %v", err)
		}
	}
	if _, err := manager.IssueLocal(ctx, stale); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("stale authenticated credentials issued a session: %v", err)
	}
	fresh, err := login.Execute(ctx, "admin", "replacement passphrase", client)
	if err != nil || fresh.Principal.MustChangePassword {
		t.Fatalf("replacement login: mustChange=%t err=%v", fresh.Principal.MustChangePassword, err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.sessions.revoke_on_password_change", Limit: 10})
	if err != nil || len(events) != 1 || events[0].ActorID != account.ID {
		t.Fatalf("password-change revocation audit count=%d err=%v", len(events), err)
	}
}

func TestSessionChangesFailClosedWhenAuditUnavailable(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_session_start BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.start' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if credentials, err := manager.IssueLocal(ctx, account); err == nil || credentials.Token != "" {
		t.Fatal("session was issued without durable audit")
	}
	var count int
	if err := db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM console_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatalf("session creation was not rolled back: count=%d err=%v", count, err)
	}
	if _, err := db.db.ExecContext(ctx, "DROP TRIGGER reject_session_start"); err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_session_logout BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.logout' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Revoke(ctx, credentials.Token); err == nil {
		t.Fatal("logout reported success without audit")
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != nil {
		t.Fatalf("failed logout did not roll back: %v", err)
	}
}

func TestPasswordChangeAndRevocationRollBackTogether(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	oldHash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	newHash, err := identity.HashLocalPassword(rand.Reader, "replacement passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: oldHash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_revocation_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.sessions.revoke_on_password_change'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	update := identity.LocalPasswordUpdate{
		AccountID: account.ID, PreviousHash: oldHash, NewHash: newHash,
		EventID: "password-change", RevocationEventID: "session-revocation", OccurredAt: time.Now(),
	}
	if err := db.UpdateLocalPassword(ctx, update); err == nil {
		t.Fatal("password change succeeded without revocation audit")
	}
	stored, err := db.FindLocalAccount(ctx, account.Username)
	if err != nil || stored.PasswordHash != oldHash || stored.CredentialVersion != 1 || !stored.MustChangePassword {
		t.Fatal("password update did not roll back with revocation audit")
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != nil {
		t.Fatalf("session revocation did not roll back: %v", err)
	}
}

func TestDisabledAccountCannotUseOrCreateSessions(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("disabled identity used a session: %v", err)
	}
	if _, err := manager.IssueLocal(ctx, account); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("disabled identity created a session: %v", err)
	}
}
