package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestLocalAuthenticationAndAudit(t *testing.T) {
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
	const username, password = "admin-example", "a strong passphrase"
	created, err := bootstrap.Execute(ctx, secret, username, password)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.1")
	account, err := authenticator.Execute(ctx, "ADMIN-EXAMPLE", password, client)
	if err != nil || account.ID != created.ID || !account.Enabled || !account.MustChangePassword || account.PasswordHash != "" {
		t.Fatalf("valid local authentication: id=%q enabled=%t mustChange=%t err=%v",
			account.ID, account.Enabled, account.MustChangePassword, err)
	}
	for _, tt := range []struct{ user, password string }{
		{username, "incorrect password"},
		{"unknown-example", password},
		{"invalid user", password},
	} {
		if got, err := authenticator.Execute(ctx, tt.user, tt.password, client); !errors.Is(err, identity.ErrAuthenticationFailed) || got.ID != "" {
			t.Fatalf("invalid authentication returned an account or unexpected error: %v", err)
		}
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", created.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := authenticator.Execute(ctx, username, password, client); !errors.Is(err, identity.ErrAuthenticationFailed) || got.ID != "" {
		t.Fatalf("disabled identity authenticated: %v", err)
	}
	if _, err := authenticator.Execute(ctx, username, password, netip.Addr{}); !errors.Is(err, identity.ErrInvalidClientAddress) {
		t.Fatalf("invalid client address: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 11 {
		t.Fatalf("audit event count = %d, want 11", len(events))
	}
	for i := 1; i < len(events); i += 2 {
		attempt, result := events[i], events[i+1]
		if attempt.Outcome != AuditAttempted || attempt.OperationID == "" || attempt.OperationID != result.OperationID {
			t.Fatalf("uncorrelated authentication audit at %d", i)
		}
		if i == 1 {
			if result.Outcome != AuditSucceeded || result.ActorID != created.ID {
				t.Fatal("successful authentication lacks user attribution")
			}
		} else if result.Outcome != AuditFailed || result.ActorID != "anonymous" || result.Source != AuditSourceAnonymous {
			t.Fatal("failed authentication is not anonymous")
		}
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, sensitive := range []string{username, password, secret, "incorrect password", "unknown-example", "192.0.2.1"} {
		if strings.Contains(serialized, sensitive) {
			t.Fatal("authentication audit contains credential or untrusted input")
		}
	}
}

func TestLocalAuthenticationRequiresDurableAudit(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(ctx, secret, "admin", "a strong passphrase"); err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_successful_auth BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_authentication' AND NEW.outcome = 'succeeded'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	account, err := authenticator.Execute(ctx, "admin", "a strong passphrase", netip.MustParseAddr("192.0.2.1"))
	if err == nil || account.ID != "" {
		t.Fatal("authentication succeeded when the audit outcome could not be persisted")
	}
}

func TestAuthenticationThrottleSurvivesReopenAndExpires(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	attempt := identity.LocalAuthenticationAttempt{OccurredAt: at}
	attempt.PrincipalKey[0], attempt.ClientKey[0] = 1, 2
	for i := 0; i < localAuthenticationPrincipalLimit; i++ {
		attempt.EventID = fmt.Sprintf("attempt-%d", i)
		if err := db.ReserveLocalAuthentication(ctx, attempt); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	attempt.EventID = "throttled"
	if err := db.ReserveLocalAuthentication(ctx, attempt); !errors.Is(err, identity.ErrAuthenticationThrottled) {
		t.Fatalf("after reopen: %v", err)
	}
	attempt.OccurredAt = at.Add(localAuthenticationWindow)
	attempt.EventID = "expired"
	if err := db.ReserveLocalAuthentication(ctx, attempt); err != nil {
		t.Fatalf("after window expiry: %v", err)
	}
}

func TestLocalAuthenticationRateLimits(t *testing.T) {
	for _, tt := range []struct {
		name  string
		limit int
		scope string
	}{
		{"principal", localAuthenticationPrincipalLimit, "principal"},
		{"client", localAuthenticationClientLimit, "client"},
		{"global", localAuthenticationGlobalLimit, "global"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			at := time.Now().UTC()
			tx, err := db.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < tt.limit-1; i++ {
				var principal, client [32]byte
				if tt.scope != "principal" {
					binary.BigEndian.PutUint32(principal[:4], uint32(i+1))
				}
				if tt.scope != "client" {
					binary.BigEndian.PutUint32(client[:4], uint32(i+1))
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO local_auth_attempts VALUES (?, ?, ?)", principal[:], client[:], at.UnixNano()); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			attempt := identity.LocalAuthenticationAttempt{EventID: "last-allowed", OccurredAt: at}
			if err := db.ReserveLocalAuthentication(ctx, attempt); err != nil {
				t.Fatalf("last permitted attempt: %v", err)
			}
			attempt.EventID = "first-denied"
			if err := db.ReserveLocalAuthentication(ctx, attempt); !errors.Is(err, identity.ErrAuthenticationThrottled) {
				t.Fatalf("first denied attempt: %v", err)
			}
		})
	}
}

func TestConcurrentAuthenticationReservationsRespectLimit(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const count = localAuthenticationPrincipalLimit * 2
	results := make(chan error, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			attempt := identity.LocalAuthenticationAttempt{EventID: fmt.Sprintf("attempt-%d", i), OccurredAt: time.Now().UTC()}
			results <- db.ReserveLocalAuthentication(ctx, attempt)
		}(i)
	}
	group.Wait()
	close(results)
	allowed, denied := 0, 0
	for err := range results {
		switch {
		case err == nil:
			allowed++
		case errors.Is(err, identity.ErrAuthenticationThrottled):
			denied++
		default:
			t.Errorf("unexpected reservation error: %v", err)
		}
	}
	if allowed != localAuthenticationPrincipalLimit || denied != count-localAuthenticationPrincipalLimit {
		t.Fatalf("allowed=%d denied=%d", allowed, denied)
	}
}

func TestAuthenticationRechecksDisabledOrChangedCredentials(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	hash, err := identity.HashLocalPassword(rand.Reader, "a strong passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", time.Now()); err != nil {
		t.Fatal(err)
	}
	result := identity.LocalAuthenticationResult{AccountID: account.ID, PasswordHash: hash, CredentialVersion: 1, EventID: "disabled", OperationID: "attempt", OccurredAt: time.Now()}
	if _, err := db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", account.ID); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.FinishLocalAuthentication(ctx, result); err != nil || ok {
		t.Fatalf("disabled identity: authenticated=%t err=%v", ok, err)
	}
	newHash, err := identity.HashLocalPassword(rand.Reader, "a different passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE identities SET enabled = 1 WHERE id = ?", account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE local_accounts SET password_hash = ? WHERE identity_id = ?", newHash, account.ID); err != nil {
		t.Fatal(err)
	}
	result.EventID = "changed"
	if ok, err := db.FinishLocalAuthentication(ctx, result); err != nil || ok {
		t.Fatalf("changed credential: authenticated=%t err=%v", ok, err)
	}
	if _, err := db.db.ExecContext(ctx, "UPDATE local_accounts SET password_hash = ?, credential_version = 2 WHERE identity_id = ?", hash, account.ID); err != nil {
		t.Fatal(err)
	}
	result.EventID = "version-changed"
	if ok, err := db.FinishLocalAuthentication(ctx, result); err != nil || ok {
		t.Fatalf("changed credential version: authenticated=%t err=%v", ok, err)
	}
}
