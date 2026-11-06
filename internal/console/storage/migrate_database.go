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
	if version > 2 {
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
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database migration: %w", err)
	}
	return nil
}
