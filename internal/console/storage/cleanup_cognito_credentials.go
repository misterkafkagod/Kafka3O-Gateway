package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const CognitoCleanupBatchSize = 100

func (d *Database) CleanupCognitoCredentials(ctx context.Context, operationID string, requested time.Time) (count int, result error) {
	event := AuditEvent{
		EventID: operationID + ".000", OperationID: operationID, OccurredAt: requested,
		ActorID: "service", Source: AuditSourceService, Action: "console.cognito.credentials.cleanup",
		ResourceKind: "session", Decision: "allowed", Outcome: AuditSucceeded,
	}
	if ctx == nil || strings.TrimSpace(operationID) == "" || len(operationID) > 120 || validateAuditEvent(event) != nil {
		return 0, ErrCognitoCleanupConfig
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin Cognito credential cleanup: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			count = 0
			result = errors.Join(ErrCognitoCleanupUnknown, result, fmt.Errorf("roll back Cognito credential cleanup: %w", err))
		}
	}()
	now, err := d.transactionTime(requested)
	if err != nil {
		return 0, err
	}
	event.OccurredAt = now
	rows, err := tx.QueryContext(ctx, `DELETE FROM cognito_session_credentials WHERE session_id IN (
SELECT credentials.session_id FROM cognito_session_credentials AS credentials
JOIN console_sessions AS sessions ON sessions.id = credentials.session_id
JOIN identities ON identities.id = sessions.identity_id
WHERE sessions.auth_source = 'cognito' AND (
  sessions.revoked_at_ns IS NOT NULL OR identities.enabled = 0 OR
  sessions.idle_expires_at_ns <= ? OR sessions.absolute_expires_at_ns <= ?
)
ORDER BY credentials.session_id LIMIT ?
) RETURNING session_id`, now.UnixNano(), now.UnixNano(), CognitoCleanupBatchSize)
	if err != nil {
		return 0, fmt.Errorf("remove inactive Cognito credentials: %w", err)
	}
	var sessions []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, errors.Join(fmt.Errorf("read cleaned Cognito session: %w", err), rows.Close())
		}
		sessions = append(sessions, id)
	}
	if err := rows.Err(); err != nil {
		return 0, errors.Join(fmt.Errorf("read cleaned Cognito sessions: %w", err), rows.Close())
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close Cognito cleanup results: %w", err)
	}
	for index, id := range sessions {
		if _, err := tx.ExecContext(ctx, `UPDATE console_sessions SET revoked_at_ns = ?
WHERE id = ? AND revoked_at_ns IS NULL`, now.UnixNano(), id); err != nil {
			return 0, fmt.Errorf("revoke inactive Cognito session: %w", err)
		}
		event.EventID, event.ResourceID = fmt.Sprintf("%s.%03d", operationID, index), id
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return 0, fmt.Errorf("audit Cognito credential cleanup: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.Join(ErrCognitoCleanupUnknown, fmt.Errorf("commit Cognito credential cleanup: %w", err))
	}
	return len(sessions), nil
}
