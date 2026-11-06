package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) RevokeSession(ctx context.Context, tokenHash [32]byte, eventID string, now time.Time) (result error) {
	event := AuditEvent{
		EventID: eventID, OperationID: eventID, OccurredAt: now, ActorID: "anonymous",
		Source: AuditSourceLocal, Action: "console.session.logout", Decision: "allowed", Outcome: AuditSucceeded,
	}
	if tokenHash == ([32]byte{}) {
		return session.ErrInvalidSession
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin session revocation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back session revocation: %w", err))
		}
	}()
	err = tx.QueryRowContext(ctx, "SELECT id, identity_id FROM console_sessions WHERE token_hash = ? AND revoked_at_ns IS NULL",
		tokenHash[:]).Scan(&event.ResourceID, &event.ActorID)
	if errors.Is(err, sql.ErrNoRows) {
		return session.ErrInvalidSession
	}
	if err != nil {
		return fmt.Errorf("find session for revocation: %w", err)
	}
	event.ResourceKind = "session"
	if _, err := tx.ExecContext(ctx, "UPDATE console_sessions SET revoked_at_ns = ? WHERE token_hash = ? AND revoked_at_ns IS NULL",
		now.UTC().UnixNano(), tokenHash[:]); err != nil {
		return fmt.Errorf("revoke local session: %w", err)
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit session revocation: %w", err)
	}
	return nil
}
