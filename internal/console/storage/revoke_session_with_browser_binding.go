package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) revokeSessionWithBrowserBinding(ctx context.Context, tokenHash, browserHash [32]byte, eventID string, now time.Time) (result error) {
	event := AuditEvent{
		EventID: eventID, OperationID: eventID, OccurredAt: now, ActorID: "anonymous",
		Source: AuditSourceLocal, Action: "console.session.logout", Decision: "allowed", Outcome: AuditSucceeded,
	}
	if ctx == nil || tokenHash == ([32]byte{}) {
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
			if browserHash != ([32]byte{}) {
				result = errors.Join(session.ErrBrowserSessionUnknown, result)
			}
			result = errors.Join(result, fmt.Errorf("roll back session revocation: %w", err))
		}
	}()
	if browserHash != ([32]byte{}) {
		now, err = d.strictTransactionTime(now)
		if err != nil {
			return err
		}
		event.OccurredAt = now
	}
	var source string
	var created int64
	bindingPredicate, bindingArgs := browserSessionPredicate("console_sessions", browserHash)
	args := []any{tokenHash[:]}
	args = append(args, bindingArgs...)
	err = tx.QueryRowContext(ctx, `SELECT id, identity_id, auth_source, created_at_ns FROM console_sessions
WHERE token_hash = ? AND `+bindingPredicate+` AND revoked_at_ns IS NULL`, args...).Scan(
		&event.ResourceID, &event.ActorID, &source, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return session.ErrInvalidSession
	}
	if err != nil {
		return fmt.Errorf("find session for revocation: %w", err)
	}
	if browserHash != ([32]byte{}) {
		if created > now.UnixNano() {
			return ErrInvalidAuditEvent
		}
		now, err = d.strictTransactionTime(now)
		if err != nil {
			return err
		}
		event.OccurredAt = now
	}
	if source == string(session.SourceCognito) {
		event.Source = AuditSourceCognito
	} else if source != string(session.SourceLocal) {
		return session.ErrInvalidSession
	}
	event.ResourceKind = "session"
	args = []any{now.UTC().UnixNano(), tokenHash[:]}
	args = append(args, bindingArgs...)
	updated, err := tx.ExecContext(ctx, `UPDATE console_sessions SET revoked_at_ns = ?
WHERE token_hash = ? AND `+bindingPredicate+` AND revoked_at_ns IS NULL`, args...)
	if err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	if rows, err := updated.RowsAffected(); err != nil {
		return fmt.Errorf("inspect session revocation: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("session changed during revocation")
	}
	if source == string(session.SourceCognito) {
		if _, err := tx.ExecContext(ctx, "DELETE FROM cognito_session_credentials WHERE session_id = ?", event.ResourceID); err != nil {
			return fmt.Errorf("remove revoked Cognito credentials: %w", err)
		}
		var retained bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM cognito_session_credentials WHERE session_id = ?)", event.ResourceID).Scan(&retained); err != nil {
			return fmt.Errorf("verify revoked Cognito credential removal: %w", err)
		}
		if retained {
			return fmt.Errorf("revoked Cognito credentials were not removed")
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		if browserHash != ([32]byte{}) {
			return browserSessionOutcomeUnknown(fmt.Errorf("commit session revocation: %w", err))
		}
		return fmt.Errorf("commit session revocation: %w", err)
	}
	return nil
}
