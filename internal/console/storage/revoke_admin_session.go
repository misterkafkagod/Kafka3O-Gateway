package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) RevokeAdminSession(ctx context.Context, revocation identity.AdminSessionRevocation) (result error) {
	attempt := AuditEvent{
		EventID: revocation.OperationID, OperationID: revocation.OperationID, OccurredAt: revocation.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.session.admin_revoke", Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(attempt); err != nil || revocation.EventID == "" || revocation.EventID == revocation.OperationID {
		return ErrInvalidAuditEvent
	}
	if err := d.AppendAuditEvent(ctx, attempt); err != nil {
		return errors.Join(identity.ErrAdminSessionRevokeAudit, fmt.Errorf("record session revocation attempt: %w", err))
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin session revocation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(identity.ErrAdminSessionRevokeUnknown, result, fmt.Errorf("roll back session revocation: %w", err))
		}
	}()
	revocation.OccurredAt, err = d.transactionTime(revocation.OccurredAt)
	if err != nil {
		return err
	}
	event := AuditEvent{
		EventID: revocation.EventID, OperationID: revocation.OperationID, OccurredAt: revocation.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.session.admin_revoke",
		Decision: "denied", Outcome: AuditDenied,
	}
	authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, revocation.Actor, revocation.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(authorizationErr, identity.ErrInvalidAdminSession):
		decisionErr = authorizationErr
	case errors.Is(authorizationErr, identity.ErrGlobalAdminRequired), errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
		event.ActorID, event.Source = revocation.Actor.AccountID, AuditSourceLocal
		decisionErr = authorizationErr
	case authorizationErr != nil:
		return authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = revocation.Actor.AccountID, AuditSourceLocal, "allowed"
		if err := identity.ValidateSessionID(revocation.SessionID); err != nil {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidSessionID
			break
		}
		var targetExists int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM console_sessions
JOIN identities ON identities.id = console_sessions.identity_id
JOIN local_accounts ON local_accounts.identity_id = identities.id
WHERE console_sessions.id = ? AND console_sessions.revoked_at_ns IS NULL
AND console_sessions.idle_expires_at_ns > ? AND console_sessions.absolute_expires_at_ns > ?
AND identities.enabled = 1 AND local_accounts.credential_version = console_sessions.credential_version`,
			revocation.SessionID, revocation.OccurredAt.UnixNano(), revocation.OccurredAt.UnixNano()).Scan(&targetExists)
		if errors.Is(err, sql.ErrNoRows) {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrAdminSessionUnavailable
			break
		}
		if err != nil {
			return fmt.Errorf("inspect target session: %w", err)
		}
		updated, err := tx.ExecContext(ctx, "UPDATE console_sessions SET revoked_at_ns = ? WHERE id = ? AND revoked_at_ns IS NULL",
			revocation.OccurredAt.UnixNano(), revocation.SessionID)
		if err != nil {
			return fmt.Errorf("revoke target session: %w", err)
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect session revocation: %w", err)
		}
		if count != 1 {
			return errors.New("target session changed during revocation")
		}
		event.ResourceKind, event.ResourceID, event.Outcome = "session", revocation.SessionID, AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return errors.Join(identity.ErrAdminSessionRevokeAudit, fmt.Errorf("record session revocation outcome: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(identity.ErrAdminSessionRevokeUnknown, fmt.Errorf("commit session revocation: %w", err))
	}
	return decisionErr
}
