package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

const (
	browserSessionRetirementAction = "console.session.retire"
	browserBindingCleanupAction    = "console.browser.binding.cleanup"
)

func retireBrowserSession(ctx context.Context, tx *sql.Tx, previous browserSessionCheck, occurredAt time.Time, operationID, action string) error {
	if previous.sessionID == "" {
		return nil
	}
	if occurredAt.Before(previous.checkedAt) {
		return ErrInvalidAuditEvent
	}
	if !previous.revoked {
		updated, err := tx.ExecContext(ctx, "UPDATE console_sessions SET revoked_at_ns = ? WHERE id = ? AND revoked_at_ns IS NULL",
			occurredAt.UnixNano(), previous.sessionID)
		if err != nil {
			return fmt.Errorf("retire inactive browser session: %w", err)
		}
		if count, err := updated.RowsAffected(); err != nil {
			return fmt.Errorf("inspect browser session retirement: %w", err)
		} else if count != 1 {
			return fmt.Errorf("browser session changed during retirement")
		}
	}
	var revoked sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT revoked_at_ns FROM console_sessions WHERE id = ?", previous.sessionID).Scan(&revoked); err != nil {
		return fmt.Errorf("verify browser session retirement: %w", err)
	}
	if !revoked.Valid || revoked.Int64 > occurredAt.UnixNano() || (!previous.revoked && revoked.Int64 != occurredAt.UnixNano()) {
		return fmt.Errorf("browser session was not retired")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM cognito_session_credentials WHERE session_id = ?", previous.sessionID); err != nil {
		return fmt.Errorf("remove retired browser session credentials: %w", err)
	}
	var retained bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM cognito_session_credentials WHERE session_id = ?)", previous.sessionID).Scan(&retained); err != nil {
		return fmt.Errorf("verify retired browser session credential removal: %w", err)
	}
	if retained {
		return fmt.Errorf("retired browser session credentials were not removed")
	}
	digest := sha256.Sum256([]byte("Kafka3O browser session retirement audit v2\x00" +
		action + "\x00" + operationID + "\x00" + previous.sessionID))
	event := AuditEvent{
		EventID:     "browser-retirement-" + base64.RawURLEncoding.EncodeToString(digest[:]),
		OperationID: operationID, OccurredAt: occurredAt, ActorID: "service", Source: AuditSourceService,
		Action: action, ResourceKind: "session", ResourceID: previous.sessionID,
		Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(session.ErrBrowserSessionAudit, fmt.Errorf("audit browser session retirement: %w", err))
	}
	return nil
}
