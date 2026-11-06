package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestChangeLocalPasswordReplacesBootstrapCredential(t *testing.T) {
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
	change, err := identity.NewChangeLocalPassword(authenticator, db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.1")
	for _, tt := range []struct {
		id, current, next string
		want              error
	}{
		{account.ID, "wrong passphrase", "replacement passphrase", identity.ErrAuthenticationFailed},
		{"different-id", "initial passphrase", "replacement passphrase", identity.ErrAuthenticationFailed},
		{account.ID, "initial passphrase", "initial passphrase", identity.ErrPasswordUnchanged},
		{account.ID, "initial passphrase", "short", identity.ErrInvalidPassword},
	} {
		if err := change.Execute(ctx, tt.id, "admin", tt.current, tt.next, client); !errors.Is(err, tt.want) {
			t.Fatalf("rejected password change: got %v, want %v", err, tt.want)
		}
	}
	if err := change.Execute(ctx, account.ID, "admin", "initial passphrase", "replacement passphrase", client); err != nil {
		t.Fatal(err)
	}
	stored, err := db.FindLocalAccount(ctx, "admin")
	if err != nil || stored.MustChangePassword {
		t.Fatalf("updated account: mustChange=%t err=%v", stored.MustChangePassword, err)
	}
	if _, err := authenticator.Execute(ctx, "admin", "initial passphrase", client); !errors.Is(err, identity.ErrAuthenticationFailed) {
		t.Fatalf("old password still accepted: %v", err)
	}
	got, err := authenticator.Execute(ctx, "admin", "replacement passphrase", client)
	if err != nil || got.ID != account.ID || got.MustChangePassword {
		t.Fatalf("replacement password: id=%q mustChange=%t err=%v", got.ID, got.MustChangePassword, err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_change", Limit: 10})
	if err != nil || len(events) != 1 || events[0].ActorID != account.ID || events[0].Outcome != AuditSucceeded {
		t.Fatalf("password-change audit count=%d err=%v", len(events), err)
	}
}

func TestPasswordChangeRollsBackWhenAuditFails(t *testing.T) {
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
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: oldHash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_password_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_password_change'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	update := identity.LocalPasswordUpdate{
		AccountID: account.ID, PreviousHash: oldHash, NewHash: newHash, EventID: "change", OccurredAt: time.Now(),
	}
	if err := db.UpdateLocalPassword(ctx, update); err == nil {
		t.Fatal("password change succeeded without audit")
	}
	stored, err := db.FindLocalAccount(ctx, "admin")
	if err != nil || stored.PasswordHash != oldHash || !stored.MustChangePassword {
		t.Fatal("failed audit did not roll back password change")
	}
}

func TestPasswordChangeRejectsStaleCredential(t *testing.T) {
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
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: oldHash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	first := identity.LocalPasswordUpdate{
		AccountID: account.ID, PreviousHash: oldHash, NewHash: newHash, EventID: "first", OccurredAt: time.Now(),
	}
	if err := db.UpdateLocalPassword(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.EventID = "stale"
	if err := db.UpdateLocalPassword(ctx, first); !errors.Is(err, identity.ErrAuthenticationFailed) {
		t.Fatalf("stale update: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_change", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditFailed {
		t.Fatalf("stale-update audit count=%d err=%v", len(events), err)
	}
}
