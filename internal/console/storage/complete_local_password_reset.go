package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) CompleteLocalPasswordReset(ctx context.Context, completion identity.LocalPasswordResetCompletion) (result error) {
	if err := identity.ValidateLocalPasswordHash(completion.NewHash); err != nil {
		return err
	}
	event := AuditEvent{
		EventID: completion.EventID, OperationID: completion.OperationID, OccurredAt: completion.OccurredAt,
		ActorID: completion.Actor.AccountID, Source: AuditSourceLocal, Action: "console.local_password_reset",
		ResourceKind: "local_account", ResourceID: completion.TargetID, Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := validateAuditEvent(event); err != nil || completion.OperationID == "" || completion.RevocationEventID == "" ||
		completion.OperationID == completion.EventID || completion.OperationID == completion.RevocationEventID ||
		completion.EventID == completion.RevocationEventID {
		return ErrInvalidAuditEvent
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local password reset: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local password reset: %w", err))
		}
	}()
	completion.OccurredAt, err = d.transactionTime(completion.OccurredAt)
	if err != nil {
		return err
	}
	event.OccurredAt = completion.OccurredAt
	version, consumeErr := consumeLocalPasswordReset(ctx, tx, completion.Actor, completion.TargetID, completion.OperationID, completion.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(consumeErr, identity.ErrPasswordResetExpired):
		event.Decision, event.Outcome, decisionErr = "denied", AuditDenied, consumeErr
	case consumeErr != nil:
		return consumeErr
	default:
		authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, completion.Actor, completion.OccurredAt)
		switch {
		case errors.Is(authorizationErr, identity.ErrInvalidAdminSession),
			errors.Is(authorizationErr, identity.ErrGlobalAdminRequired),
			errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
			event.Decision, event.Outcome, decisionErr = "denied", AuditDenied, authorizationErr
		case authorizationErr != nil:
			return authorizationErr
		default:
			updated, err := tx.ExecContext(ctx, `UPDATE local_accounts SET password_hash = ?, must_change_password = 1,
credential_version = credential_version + 1 WHERE identity_id = ? AND credential_version = ?`,
				completion.NewHash, completion.TargetID, version)
			if err != nil {
				return fmt.Errorf("replace local password: %w", err)
			}
			count, err := updated.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect local password reset: %w", err)
			}
			if count != 1 {
				event.Outcome, decisionErr = AuditValidationRejected, identity.ErrLocalAccountChanged
				break
			}
			revoked, err := tx.ExecContext(ctx, `UPDATE console_sessions SET revoked_at_ns = ?
WHERE identity_id = ? AND revoked_at_ns IS NULL`, completion.OccurredAt.UTC().UnixNano(), completion.TargetID)
			if err != nil {
				return fmt.Errorf("revoke reset account sessions: %w", err)
			}
			revokedCount, err := revoked.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect reset account sessions: %w", err)
			}
			if revokedCount > 0 {
				revocation := AuditEvent{
					EventID: completion.RevocationEventID, OperationID: completion.OperationID, OccurredAt: completion.OccurredAt,
					ActorID: completion.Actor.AccountID, Source: AuditSourceLocal,
					Action: "console.sessions.revoke_on_password_reset", ResourceKind: "local_account",
					ResourceID: completion.TargetID, Decision: "allowed", Outcome: AuditSucceeded,
				}
				if err := insertAuditEvent(ctx, tx, revocation); err != nil {
					return err
				}
			}
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(identity.ErrPasswordResetUnknown, fmt.Errorf("commit local password reset: %w", err))
	}
	return decisionErr
}
