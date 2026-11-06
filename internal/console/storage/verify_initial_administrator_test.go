package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestVerifyInitialAdministratorRequiresEnabledLocalGlobalAdmin(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.VerifyInitialAdministrator(ctx); !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("uninitialized database: %v", err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "admin-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("enabled initial administrator: %v", err)
	}
	for _, statement := range []string{
		"UPDATE identities SET enabled = 0 WHERE id = 'admin-1'",
		"UPDATE identities SET enabled = 1 WHERE id = 'admin-1'; DELETE FROM role_grants WHERE account_id = 'admin-1'",
	} {
		if _, err := db.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
		if err := db.VerifyInitialAdministrator(ctx); !errors.Is(err, ErrNoLocalGlobalAdmin) {
			t.Fatalf("missing enabled global administrator: %v", err)
		}
	}
}
