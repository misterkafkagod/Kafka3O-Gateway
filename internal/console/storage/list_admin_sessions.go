package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) ListAdminSessions(ctx context.Context, request identity.AdminSessionListRequest) (page identity.AdminSessionListResult, result error) {
	event := AuditEvent{
		EventID: request.OperationID, OperationID: request.OperationID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.session.list",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return identity.AdminSessionListResult{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return identity.AdminSessionListResult{}, fmt.Errorf("begin session listing: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			page = identity.AdminSessionListResult{}
			result = errors.Join(identity.ErrAdminSessionListAudit, result, fmt.Errorf("roll back session listing: %w", err))
		}
	}()
	request.OccurredAt, err = d.adminTransactionTime(request.Actor, request.OccurredAt)
	if err != nil {
		return identity.AdminSessionListResult{}, err
	}
	event.OccurredAt = request.OccurredAt
	authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, request.Actor, request.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(authorizationErr, identity.ErrInvalidAdminSession):
		decisionErr = authorizationErr
	case errors.Is(authorizationErr, identity.ErrGlobalAdminRequired), errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
		event.ActorID, event.Source = request.Actor.AccountID, AuditSourceLocal
		decisionErr = authorizationErr
	case authorizationErr != nil:
		return identity.AdminSessionListResult{}, authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = request.Actor.AccountID, AuditSourceLocal, "allowed"
		if request.Limit < 1 || request.Limit > identity.MaxAdminSessionPageSize {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAdminSessionList
			break
		}
		if request.AfterID != "" && identity.ValidateSessionID(request.AfterID) != nil {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidAdminSessionCursor
			break
		}
		rows, err := tx.QueryContext(ctx, `SELECT console_sessions.id, identities.id,
CASE WHEN console_sessions.auth_source = 'cognito' THEN 'cognito-user' ELSE local_accounts.username END,
console_sessions.auth_source,
console_sessions.created_at_ns, console_sessions.idle_expires_at_ns, console_sessions.absolute_expires_at_ns
FROM console_sessions JOIN identities ON identities.id = console_sessions.identity_id
LEFT JOIN local_accounts ON local_accounts.identity_id = identities.id
LEFT JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
  AND cognito_session_credentials.identity_id = identities.id
LEFT JOIN cognito_identities ON cognito_identities.identity_id = cognito_session_credentials.identity_id
WHERE console_sessions.id > ? AND console_sessions.revoked_at_ns IS NULL
AND console_sessions.idle_expires_at_ns > ? AND console_sessions.absolute_expires_at_ns > ?
AND identities.enabled = 1 AND (
  (console_sessions.auth_source = 'local' AND local_accounts.credential_version = console_sessions.credential_version)
  OR (console_sessions.auth_source = 'cognito' AND cognito_identities.identity_id IS NOT NULL AND console_sessions.credential_version = 1)
)
ORDER BY console_sessions.id ASC LIMIT ?`, request.AfterID, request.OccurredAt.UnixNano(), request.OccurredAt.UnixNano(), request.Limit+1)
		if err != nil {
			return identity.AdminSessionListResult{}, fmt.Errorf("query active sessions: %w", err)
		}
		defer rows.Close()
		page.Sessions = make([]identity.AdminSessionSummary, 0, request.Limit+1)
		for rows.Next() {
			var item identity.AdminSessionSummary
			var created, idle, absolute int64
			if err := rows.Scan(&item.ID, &item.AccountID, &item.Username, &item.Source, &created, &idle, &absolute); err != nil {
				return identity.AdminSessionListResult{}, fmt.Errorf("read active session: %w", err)
			}
			normalized, nameErr := identity.NormalizeLocalUsername(item.Username)
			if identity.ValidateSessionID(item.ID) != nil || item.AccountID == "" ||
				identity.ValidateLocalAccountCursor(item.AccountID) != nil || nameErr != nil || normalized != item.Username ||
				(item.Source != "local" && item.Source != "cognito") ||
				created >= idle || idle > absolute {
				return identity.AdminSessionListResult{}, errors.New("stored session has invalid metadata")
			}
			item.CreatedAt = time.Unix(0, created).UTC()
			item.IdleExpiresAt = time.Unix(0, idle).UTC()
			item.AbsoluteExpiresAt = time.Unix(0, absolute).UTC()
			item.Current = item.ID == request.Actor.SessionID
			page.Sessions = append(page.Sessions, item)
		}
		if err := rows.Err(); err != nil {
			return identity.AdminSessionListResult{}, fmt.Errorf("iterate active local sessions: %w", err)
		}
		if err := rows.Close(); err != nil {
			return identity.AdminSessionListResult{}, fmt.Errorf("close active session listing: %w", err)
		}
		if len(page.Sessions) > request.Limit {
			page.Sessions = page.Sessions[:request.Limit]
			page.NextCursor = page.Sessions[len(page.Sessions)-1].ID
		}
		event.Outcome = AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return identity.AdminSessionListResult{}, errors.Join(identity.ErrAdminSessionListAudit, fmt.Errorf("record session listing: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return identity.AdminSessionListResult{}, errors.Join(identity.ErrAdminSessionListAudit, fmt.Errorf("commit session listing audit: %w", err))
	}
	if decisionErr != nil {
		return identity.AdminSessionListResult{}, decisionErr
	}
	page.OperationID = request.OperationID
	return page, nil
}
