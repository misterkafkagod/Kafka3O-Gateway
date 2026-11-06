package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestOpenDatabaseCreatesDurableDatabaseWithRequiredSettings(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console # database.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var journal string
	var sync, foreignKeys, version int
	for query, destination := range map[string]any{
		"PRAGMA journal_mode": &journal, "PRAGMA synchronous": &sync,
		"PRAGMA foreign_keys": &foreignKeys, "PRAGMA user_version": &version,
	} {
		if err := db.db.QueryRowContext(ctx, query).Scan(destination); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if journal != "wal" || sync != 2 || foreignKeys != 1 || version != 7 {
		t.Fatalf("journal=%q synchronous=%d foreignKeys=%d version=%d", journal, sync, foreignKeys, version)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		t.Fatalf("database permissions = %o", info.Mode().Perm())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDatabaseRejectsUnsafePaths(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"", ":memory:", "relative.sqlite", t.TempDir()} {
		db, err := OpenDatabase(ctx, path)
		if db != nil || !errors.Is(err, ErrInvalidDatabasePath) {
			t.Errorf("path %q: got db=%v err=%v", path, db, err)
		}
	}
}

func TestOpenDatabaseRejectsInsecureExistingFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows uses ACLs rather than Unix permission bits")
	}
	path := filepath.Join(t.TempDir(), "existing.sqlite")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(context.Background(), path)
	if db != nil || !errors.Is(err, ErrInsecureDatabase) {
		t.Fatalf("got db=%v err=%v", db, err)
	}
}

func TestOpenDatabaseRejectsUnsupportedSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, "PRAGMA user_version = 999"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if db != nil || !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("got db=%v err=%v", db, err)
	}
}

func TestOpenDatabaseMigratesVersionOneWithoutLosingAudit(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	event := AuditEvent{EventID: "old-event", OccurredAt: time.Now().UTC(), ActorID: "user", Source: AuditSourceLocal, Action: "login", Outcome: AuditSucceeded}
	if err := db.AppendAuditEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DROP TABLE console_sessions", "DROP TABLE role_grants", "DROP TABLE local_accounts", "DROP TABLE identities",
		"DROP TABLE bootstrap_state", "DROP TABLE bootstrap_attempts", "DROP TABLE password_reset_attempts",
		"DROP TABLE local_account_creation_attempts", "DROP TABLE local_auth_attempts", "DROP TABLE cognito_authorization_attempts", "PRAGMA user_version = 1",
	} {
		if _, err := db.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
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
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
	if err != nil || len(events) != 1 || events[0].EventID != "old-event" {
		t.Fatalf("events after migration = %+v, %v", events, err)
	}
	var version int
	if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 7 {
		t.Fatalf("schema version = %d, %v", version, err)
	}
}

func TestOpenDatabaseMigratesVersionTwoWithoutLosingAccounts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DROP TABLE console_sessions", "DROP TABLE bootstrap_attempts", "DROP TABLE password_reset_attempts", "DROP TABLE local_account_creation_attempts", "DROP TABLE cognito_authorization_attempts",
		"ALTER TABLE local_accounts DROP COLUMN credential_version", "PRAGMA user_version = 2",
	} {
		if _, err := db.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
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
	stored, err := db.FindLocalAccount(ctx, "admin")
	if err != nil || stored.ID != account.ID || stored.PasswordHash != hash || stored.CredentialVersion != 1 {
		t.Fatalf("account was not preserved during migration: err=%v", err)
	}
	grants, err := db.ListAccountGrants(ctx, account.ID)
	if err != nil || len(grants) != 1 || grants[0].Role != authorization.RoleAdmin {
		t.Fatalf("grant was not preserved during migration: err=%v", err)
	}
}

func TestOpenDatabaseMigratesVersionThreeWithoutLosingSessions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	account := identity.LocalAccount{
		ID: "user-1", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1,
	}
	if err := db.CreateInitialAdmin(ctx, account, "bootstrap", "bootstrap-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"DROP TABLE bootstrap_attempts", "DROP TABLE password_reset_attempts", "DROP TABLE local_account_creation_attempts", "DROP TABLE cognito_authorization_attempts", "PRAGMA user_version = 3"} {
		if _, err := db.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
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
	manager, err = session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := manager.Resolve(ctx, credentials.Token)
	if err != nil || principal.AccountID != account.ID {
		t.Fatalf("session was not preserved during migration: %v", err)
	}
	if err := db.ReserveBootstrapAttempt(ctx, identity.BootstrapAttempt{
		EventID: "post-migration", OccurredAt: time.Now(),
	}); !errors.Is(err, identity.ErrAlreadyBootstrapped) {
		t.Fatalf("bootstrap state was not preserved during migration: %v", err)
	}
}
