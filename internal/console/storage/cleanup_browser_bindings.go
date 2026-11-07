package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

const BrowserBindingCleanupBatchSize = 100

func (d *Database) CleanupBrowserBindings(ctx context.Context, operationID string, requested time.Time) (count int, result error) {
	event := AuditEvent{
		EventID: "browser-cleanup", OperationID: operationID, OccurredAt: requested,
		ActorID: "service", Source: AuditSourceService, Action: browserBindingCleanupAction,
		ResourceKind: "session", Decision: "allowed", Outcome: AuditSucceeded,
	}
	if ctx == nil || strings.TrimSpace(operationID) == "" || len(operationID) > 120 || validateAuditEvent(event) != nil {
		return 0, ErrBrowserBindingCleanupConfig
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin browser binding cleanup: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			count = 0
			result = errors.Join(ErrBrowserBindingCleanupUnknown, result, fmt.Errorf("roll back browser binding cleanup: %w", err))
		}
	}()
	now, err := d.strictTransactionTime(requested)
	if err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT browsers.browser_hash
FROM console_browser_sessions AS browsers
LEFT JOIN console_sessions AS sessions ON sessions.id = browsers.session_id
LEFT JOIN identities ON identities.id = sessions.identity_id
WHERE COALESCE(
  typeof(browsers.browser_hash) = 'blob' AND length(browsers.browser_hash) = 32 AND
  browsers.browser_hash != zeroblob(32) AND sessions.id IS NOT NULL AND identities.id IS NOT NULL AND
  typeof(sessions.id) = 'text' AND length(CAST(sessions.id AS BLOB)) BETWEEN 1 AND 128 AND
  length(trim(sessions.id)) > 0 AND sessions.auth_source IN ('local', 'cognito') AND
  typeof(sessions.browser_binding_hash) = 'blob' AND sessions.browser_binding_hash = browsers.browser_hash AND
  typeof(sessions.created_at_ns) = 'integer' AND typeof(sessions.idle_expires_at_ns) = 'integer' AND
  typeof(sessions.absolute_expires_at_ns) = 'integer' AND sessions.created_at_ns >= 0 AND
  sessions.created_at_ns <= ? AND sessions.idle_expires_at_ns > sessions.created_at_ns AND
  sessions.idle_expires_at_ns <= sessions.absolute_expires_at_ns AND
  sessions.idle_expires_at_ns > ? AND sessions.absolute_expires_at_ns > ? AND
  sessions.revoked_at_ns IS NULL AND identities.enabled = 1, 0) = 0
ORDER BY browsers.browser_hash LIMIT ?`, now.UnixNano(), now.UnixNano(), now.UnixNano(), BrowserBindingCleanupBatchSize)
	if err != nil {
		return 0, fmt.Errorf("select inactive browser bindings: %w", err)
	}
	var hashes [][32]byte
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return 0, errors.Join(fmt.Errorf("read browser binding cleanup candidate: %w", err), rows.Close())
		}
		if len(raw) != 32 {
			return 0, errors.Join(session.ErrInvalidSessionConfig, rows.Close())
		}
		var hash [32]byte
		copy(hash[:], raw)
		if hash == ([32]byte{}) {
			return 0, errors.Join(session.ErrInvalidSessionConfig, rows.Close())
		}
		hashes = append(hashes, hash)
	}
	if err := rows.Err(); err != nil {
		return 0, errors.Join(fmt.Errorf("read browser binding cleanup candidates: %w", err), rows.Close())
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close browser binding cleanup candidates: %w", err)
	}
	for _, hash := range hashes {
		previous, err := d.checkBrowserSession(ctx, tx, hash, now)
		if err == session.ErrBrowserSessionConflict {
			return 0, fmt.Errorf("inconsistent browser binding cleanup eligibility: %w", session.ErrInvalidSessionConfig)
		}
		if err != nil {
			return 0, err
		}
		if previous.sessionID == "" {
			return 0, fmt.Errorf("browser binding disappeared during cleanup: %w", session.ErrInvalidSessionConfig)
		}
		now = previous.checkedAt
		if err := retireBrowserSession(ctx, tx, previous, now, operationID, browserBindingCleanupAction); err != nil {
			return 0, err
		}
		deleted, err := tx.ExecContext(ctx, "DELETE FROM console_browser_sessions WHERE browser_hash = ? AND session_id = ?",
			hash[:], previous.sessionID)
		if err != nil {
			return 0, fmt.Errorf("remove inactive browser binding: %w", err)
		}
		if affected, err := deleted.RowsAffected(); err != nil {
			return 0, fmt.Errorf("inspect browser binding removal: %w", err)
		} else if affected != 1 {
			return 0, fmt.Errorf("inactive browser binding was not removed")
		}
		var retained bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM console_browser_sessions WHERE browser_hash = ?)", hash[:]).Scan(&retained); err != nil {
			return 0, fmt.Errorf("verify browser binding removal: %w", err)
		}
		if retained {
			return 0, fmt.Errorf("inactive browser binding was retained")
		}
	}
	now, err = d.strictTransactionTime(now)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.Join(ErrBrowserBindingCleanupUnknown, fmt.Errorf("commit browser binding cleanup: %w", err))
	}
	if _, err := d.strictTransactionTime(now); err != nil {
		return 0, errors.Join(ErrBrowserBindingCleanupUnknown, err)
	}
	return len(hashes), nil
}
