package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

const (
	passwordResetWindow      = 15 * time.Minute
	passwordResetTargetLimit = 10
	passwordResetActorLimit  = 60
	passwordResetGlobalLimit = 300
)

func (d *Database) ReserveLocalPasswordReset(ctx context.Context, attempt identity.LocalPasswordResetAttempt) (result error) {
	event := AuditEvent{
		EventID: attempt.OperationID, OperationID: attempt.OperationID, OccurredAt: attempt.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.local_password_reset",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin password reset reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back password reset reservation: %w", err))
		}
	}()
	authorizationErr := authorizeLocalGlobalAdmin(ctx, tx, attempt.Actor, attempt.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(authorizationErr, identity.ErrInvalidAdminSession):
		decisionErr = authorizationErr
	case errors.Is(authorizationErr, identity.ErrGlobalAdminRequired), errors.Is(authorizationErr, identity.ErrAdminPasswordChange):
		event.ActorID, event.Source = attempt.Actor.AccountID, AuditSourceLocal
		decisionErr = authorizationErr
	case authorizationErr != nil:
		return authorizationErr
	default:
		event.ActorID, event.Source, event.Decision = attempt.Actor.AccountID, AuditSourceLocal, "allowed"
		var version sql.NullInt64
		if strings.TrimSpace(attempt.TargetID) == "" || len(attempt.TargetID) > 128 {
			event.Outcome, decisionErr = AuditValidationRejected, identity.ErrInvalidLocalAccount
		} else {
			err := tx.QueryRowContext(ctx, "SELECT credential_version FROM local_accounts WHERE identity_id = ?", attempt.TargetID).Scan(&version)
			if errors.Is(err, sql.ErrNoRows) {
				event.Outcome, decisionErr = AuditValidationRejected, identity.ErrAccountNotFound
			} else if err != nil {
				return fmt.Errorf("inspect password reset target: %w", err)
			} else {
				event.ResourceKind, event.ResourceID = "local_account", attempt.TargetID
			}
		}
		cutoff := attempt.OccurredAt.Add(-passwordResetWindow).UTC().UnixNano()
		if _, err := tx.ExecContext(ctx, "DELETE FROM password_reset_attempts WHERE occurred_at_ns <= ?", cutoff); err != nil {
			return fmt.Errorf("expire password reset attempts: %w", err)
		}
		actorKey := sha256.Sum256([]byte("actor:" + attempt.Actor.AccountID))
		targetKey := sha256.Sum256([]byte("invalid:" + attempt.OperationID))
		if decisionErr == nil {
			targetKey = sha256.Sum256([]byte("target:" + attempt.TargetID))
		}
		var total, actorCount, targetCount int
		err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(CASE WHEN actor_key = ? THEN 1 ELSE 0 END), 0),
COALESCE(SUM(CASE WHEN target_key = ? THEN 1 ELSE 0 END), 0)
FROM password_reset_attempts`, actorKey[:], targetKey[:]).Scan(&total, &actorCount, &targetCount)
		if err != nil {
			return fmt.Errorf("inspect password reset limits: %w", err)
		}
		if total >= passwordResetGlobalLimit || actorCount >= passwordResetActorLimit || targetCount >= passwordResetTargetLimit {
			event.Decision, event.Outcome, decisionErr = "denied", AuditDenied, identity.ErrPasswordResetThrottled
			break
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO password_reset_attempts
(operation_id, actor_key, target_key, session_id, credential_version, occurred_at_ns, completed)
VALUES (?, ?, ?, ?, ?, ?, ?)`, attempt.OperationID, actorKey[:], targetKey[:], attempt.Actor.SessionID,
			version, attempt.OccurredAt.UTC().UnixNano(), decisionErr != nil)
		if err != nil {
			return fmt.Errorf("reserve password reset: %w", err)
		}
		if decisionErr == nil {
			event.Outcome = AuditAttempted
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit password reset reservation: %w", err)
	}
	return decisionErr
}
