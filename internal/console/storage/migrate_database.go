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
	if version > 12 {
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
		version = 5
	}
	if version == 5 {
		const accountCreationAttempts = `
CREATE TABLE local_account_creation_attempts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    actor_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    occurred_at_ns INTEGER NOT NULL,
    completed INTEGER NOT NULL CHECK (completed IN (0, 1))
);
CREATE INDEX local_account_creation_attempts_time ON local_account_creation_attempts(occurred_at_ns);
PRAGMA user_version = 6;`
		if _, err := tx.ExecContext(ctx, accountCreationAttempts); err != nil {
			return fmt.Errorf("apply local account creation attempt schema: %w", err)
		}
		version = 6
	}
	if version == 6 {
		const cognitoAttempts = `
CREATE TABLE cognito_authorization_attempts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    state_hash BLOB NOT NULL UNIQUE CHECK (length(state_hash) = 32),
    binding_hash BLOB NOT NULL CHECK (length(binding_hash) = 32),
    nonce_hash BLOB NOT NULL CHECK (length(nonce_hash) = 32),
    encrypted_pkce BLOB NOT NULL CHECK (length(encrypted_pkce) = 71),
    created_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL,
    CHECK (created_at_ns < expires_at_ns)
);
CREATE INDEX cognito_authorization_attempts_expiry ON cognito_authorization_attempts(expires_at_ns);
PRAGMA user_version = 7;`
		if _, err := tx.ExecContext(ctx, cognitoAttempts); err != nil {
			return fmt.Errorf("apply Cognito authorization attempt schema: %w", err)
		}
		version = 7
	}
	if version == 7 {
		const cognitoSessions = `
ALTER TABLE console_sessions ADD COLUMN auth_source TEXT NOT NULL DEFAULT 'local' CHECK (auth_source IN ('local', 'cognito'));
CREATE TABLE cognito_identities (
    identity_id TEXT PRIMARY KEY NOT NULL REFERENCES identities(id),
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    UNIQUE (issuer, subject)
);
CREATE TABLE cognito_session_credentials (
    session_id TEXT PRIMARY KEY NOT NULL REFERENCES console_sessions(id),
    identity_id TEXT NOT NULL REFERENCES cognito_identities(identity_id),
    exchange_operation_id TEXT NOT NULL UNIQUE,
    original_nonce_hash BLOB NOT NULL CHECK (length(original_nonce_hash) = 32),
    audience_json TEXT NOT NULL,
    groups_json TEXT NOT NULL,
    issued_at_ns INTEGER NOT NULL,
    token_expires_at_ns INTEGER NOT NULL,
    claims_fresh_until_ns INTEGER NOT NULL,
    refresh_generation INTEGER NOT NULL CHECK (refresh_generation > 0),
    encrypted_refresh_token BLOB NOT NULL CHECK (length(encrypted_refresh_token) BETWEEN 29 AND 8220),
    CHECK (issued_at_ns < token_expires_at_ns AND claims_fresh_until_ns <= token_expires_at_ns)
);
CREATE INDEX cognito_session_credentials_identity ON cognito_session_credentials(identity_id);
PRAGMA user_version = 8;`
		if _, err := tx.ExecContext(ctx, cognitoSessions); err != nil {
			return fmt.Errorf("apply Cognito session schema: %w", err)
		}
		version = 8
	}
	if version == 8 {
		const cognitoGroupBindings = `
CREATE TABLE cognito_group_bindings (
    issuer TEXT COLLATE BINARY NOT NULL CHECK (length(issuer) > 0),
    group_name TEXT COLLATE BINARY NOT NULL CHECK (length(group_name) > 0),
    cluster_id TEXT COLLATE BINARY NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('VIEWER', 'OPERATOR', 'ADMIN')),
    PRIMARY KEY (issuer, group_name, cluster_id)
);
PRAGMA user_version = 9;`
		if _, err := tx.ExecContext(ctx, cognitoGroupBindings); err != nil {
			return fmt.Errorf("apply Cognito group binding schema: %w", err)
		}
		version = 9
	}
	if version == 9 {
		const browserSessions = `
ALTER TABLE console_sessions ADD COLUMN browser_binding_hash BLOB
    CHECK (browser_binding_hash IS NULL OR length(browser_binding_hash) = 32);
CREATE TABLE console_browser_sessions (
    browser_hash BLOB PRIMARY KEY NOT NULL CHECK (length(browser_hash) = 32),
    session_id TEXT NOT NULL UNIQUE REFERENCES console_sessions(id)
);
PRAGMA user_version = 10;`
		if _, err := tx.ExecContext(ctx, browserSessions); err != nil {
			return fmt.Errorf("apply browser session binding schema: %w", err)
		}
		version = 10
	}
	if version == 10 {
		const policyRevision = `
CREATE TABLE policy_state (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0)
);
INSERT INTO policy_state (id, revision) VALUES (1, 0);
ALTER TABLE audit_events ADD COLUMN before_state TEXT NOT NULL DEFAULT '';
ALTER TABLE audit_events ADD COLUMN after_state TEXT NOT NULL DEFAULT '';
PRAGMA user_version = 11;`
		if _, err := tx.ExecContext(ctx, policyRevision); err != nil {
			return fmt.Errorf("apply policy revision schema: %w", err)
		}
		version = 11
	}
	if version == 11 {
		const registeredClusters = `
CREATE TABLE registered_clusters (
    id TEXT COLLATE BINARY PRIMARY KEY NOT NULL CHECK (length(id) BETWEEN 1 AND 128),
    display_name TEXT NOT NULL CHECK (length(display_name) BETWEEN 1 AND 128),
    gateway_url TEXT NOT NULL CHECK (length(gateway_url) BETWEEN 1 AND 512),
    environment TEXT NOT NULL DEFAULT '' CHECK (environment IN ('', 'NON_PRODUCTION', 'PRODUCTION')),
    active INTEGER NOT NULL CHECK (active IN (0, 1))
);
PRAGMA user_version = 12;`
		if _, err := tx.ExecContext(ctx, registeredClusters); err != nil {
			return fmt.Errorf("apply registered cluster schema: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database migration: %w", err)
	}
	return nil
}
