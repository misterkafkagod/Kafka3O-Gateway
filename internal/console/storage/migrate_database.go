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
	if version > 1 {
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
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit database migration: %w", err)
	}
	return nil
}
