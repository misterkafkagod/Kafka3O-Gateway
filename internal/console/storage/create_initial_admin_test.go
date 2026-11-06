package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestBootstrapPersistsAccountGrantAndAuditExactlyOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(ctx, secret, "Admin", "a strong passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(ctx, secret, "other", "another strong passphrase"); !errors.Is(err, identity.ErrAlreadyBootstrapped) {
		t.Fatalf("reused bootstrap = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stored, err := db.FindLocalAccount(ctx, "ADMIN")
	if err != nil || stored.ID != account.ID || stored.Username != "admin" || !stored.Enabled || !stored.MustChangePassword {
		t.Fatalf("stored identity: id=%q username=%q enabled=%t mustChange=%t err=%v",
			stored.ID, stored.Username, stored.Enabled, stored.MustChangePassword, err)
	}
	matches, err := identity.VerifyLocalPassword("a strong passphrase", stored.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("password verification: matches=%t err=%v", matches, err)
	}
	grants, err := db.ListAccountGrants(ctx, account.ID)
	if err != nil || len(grants) != 1 || grants[0].Role != authorization.RoleAdmin || grants[0].ClusterID != "" {
		t.Fatalf("initial grants = %+v, %v", grants, err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
	if err != nil || len(events) != 1 || events[0].Action != "console.bootstrap" || events[0].ActorID != account.ID {
		t.Fatalf("bootstrap audit = %+v, %v", events, err)
	}
	if _, err := db.FindLocalAccount(ctx, "other"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("second account unexpectedly exists: %v", err)
	}
}

func TestCreateInitialAdminRollsBackWhenAuditFails(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	if err := db.AppendAuditEvent(ctx, AuditEvent{EventID: "duplicate", OccurredAt: now, ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "test", Outcome: AuditFailed}); err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "a strong passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "duplicate", now); err == nil {
		t.Fatal("administrator was created when audit insertion failed")
	}
	if _, err := db.FindLocalAccount(ctx, "admin"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("failed transaction left an account: %v", err)
	}
	if err := db.CreateInitialAdmin(ctx, account, "new-event", now); err != nil {
		t.Fatalf("bootstrap state was not rolled back: %v", err)
	}
}

func TestConcurrentBootstrapCreatesOnlyOneAdministrator(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 4
	results := make(chan error, attempts)
	var group sync.WaitGroup
	for i := 0; i < attempts; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := bootstrap.Execute(ctx, secret, "admin", "a strong passphrase")
			results <- err
		}()
	}
	group.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, identity.ErrAlreadyBootstrapped) {
			t.Errorf("unexpected bootstrap error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful bootstrap count = %d, want 1", successes)
	}
}
