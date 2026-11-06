package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func migrateDatabase(ctx context.Context, db *sql.DB) (result error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin database migration: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back database migration: %w", err))
		}
	}()

	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read database schema version: %w", err)
	}
	if version > 5 {
		return ErrUnsupportedSchema
	}
	if version == 0 {
		const schema = `
CREATE TABLE audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    operation_id TEXT NOT NULL,
    occurred_at_ns INTEGER NOT NULL,
    actor_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    policy_revision INTEGER NOT NULL
);
CREATE INDEX audit_events_cluster_sequence ON audit_events(cluster_id, sequence);
PRAGMA user_version = 1;`
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("apply database schema: %w", err)
		}
		version = 1
	}
	if version == 1 {
		const localAccounts = `
CREATE TABLE identities (
    id TEXT PRIMARY KEY NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1))
);
CREATE TABLE local_accounts (
    identity_id TEXT PRIMARY KEY NOT NULL REFERENCES identities(id),
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    must_change_password INTEGER NOT NULL CHECK (must_change_password IN (0, 1))
);
CREATE TABLE role_grants (
    account_id TEXT NOT NULL REFERENCES identities(id),
    cluster_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('VIEWER', 'OPERATOR', 'ADMIN')),
    PRIMARY KEY (account_id, cluster_id)
);
CREATE TABLE bootstrap_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    completed INTEGER NOT NULL CHECK (completed IN (0, 1))
);
INSERT INTO bootstrap_state (id, completed) VALUES (1, 0);
CREATE TABLE local_auth_attempts (
    principal_key BLOB NOT NULL,
    client_key BLOB NOT NULL,
    occurred_at_ns INTEGER NOT NULL
);
CREATE INDEX local_auth_attempts_time ON local_auth_attempts(occurred_at_ns);
PRAGMA user_version = 2;`
		if _, err := tx.ExecContext(ctx, localAccounts); err != nil {
			return fmt.Errorf("apply local account schema: %w", err)
		}
		version = 2
	}
	if version == 2 {
		const sessions = `
ALTER TABLE local_accounts ADD COLUMN credential_version INTEGER NOT NULL DEFAULT 1 CHECK (credential_version > 0);
CREATE TABLE console_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    identity_id TEXT NOT NULL REFERENCES identities(id),
    credential_version INTEGER NOT NULL CHECK (credential_version > 0),
    token_hash BLOB NOT NULL UNIQUE,
    csrf_hash BLOB NOT NULL,
    created_at_ns INTEGER NOT NULL,
    idle_expires_at_ns INTEGER NOT NULL,
    absolute_expires_at_ns INTEGER NOT NULL,
    revoked_at_ns INTEGER,
    CHECK (created_at_ns < idle_expires_at_ns AND idle_expires_at_ns <= absolute_expires_at_ns)
);
CREATE INDEX console_sessions_identity ON console_sessions(identity_id);
PRAGMA user_version = 3;`
		if _, err := tx.ExecContext(ctx, sessions); err != nil {
			return fmt.Errorf("apply session schema: %w", err)
		}
		version = 3
	}
	if version == 3 {
		const bootstrapAttempts = `
CREATE TABLE bootstrap_attempts (
    occurred_at_ns INTEGER NOT NULL
);
CREATE INDEX bootstrap_attempts_time ON bootstrap_attempts(occurred_at_ns);
PRAGMA user_version = 4;`
		if _, err := tx.ExecContext(ctx, bootstrapAttempts); err != nil {
			return fmt.Errorf("apply bootstrap attempt schema: %w", err)
		}
		version = 4
	}
	if version == 4 {
		const passwordResetAttempts = `
CREATE TABLE password_reset_attempts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    actor_key BLOB NOT NULL,
    target_key BLOB NOT NULL,
    session_id TEXT NOT NULL,
    credential_version INTEGER CHECK (credential_version > 0),
    occurred_at_ns INTEGER NOT NULL,
    completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
    CHECK (completed = 1 OR credential_version IS NOT NULL)
);
CREATE INDEX password_reset_attempts_time ON password_reset_attempts(occurred_at_ns);
PRAGMA user_version = 5;`
		if _, err := tx.ExecContext(ctx, passwordResetAttempts); err != nil {
			return fmt.Errorf("apply password reset attempt schema: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database migration: %w", err)
	}
	return nil
}
