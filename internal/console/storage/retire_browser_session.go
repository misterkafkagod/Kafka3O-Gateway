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

func retireBrowserSession(ctx context.Context, tx *sql.Tx, previous browserSessionCheck, occurredAt time.Time, operationID string) error {
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
			return fmt.Errorf("retire replaced browser session: %w", err)
		}
		if count, err := updated.RowsAffected(); err != nil {
			return fmt.Errorf("inspect replaced browser session retirement: %w", err)
		} else if count != 1 {
			return fmt.Errorf("replaced browser session changed during retirement")
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM cognito_session_credentials WHERE session_id = ?", previous.sessionID); err != nil {
		return fmt.Errorf("remove replaced browser session credentials: %w", err)
	}
	var retained bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM cognito_session_credentials WHERE session_id = ?)", previous.sessionID).Scan(&retained); err != nil {
		return fmt.Errorf("verify replaced browser session credential removal: %w", err)
	}
	if retained {
		return fmt.Errorf("replaced browser session credentials were not removed")
	}
	digest := sha256.Sum256([]byte("Kafka3O browser session retirement audit v1\x00" + operationID))
	event := AuditEvent{
		EventID:     "browser-retirement-" + base64.RawURLEncoding.EncodeToString(digest[:]),
		OperationID: operationID, OccurredAt: occurredAt, ActorID: "service", Source: AuditSourceService,
		Action: "console.session.retire", ResourceKind: "session", ResourceID: previous.sessionID,
		Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(session.ErrBrowserSessionAudit, fmt.Errorf("audit replaced browser session retirement: %w", err))
	}
	return nil
}
