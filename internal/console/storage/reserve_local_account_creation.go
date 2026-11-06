package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

const localAccountCreationWindow = 15 * time.Minute

func (d *Database) ReserveLocalAccountCreation(ctx context.Context, attempt identity.LocalAccountCreationAttempt) (result error) {
	if strings.TrimSpace(attempt.AccountID) == "" || len(attempt.AccountID) > 128 {
		return identity.ErrInvalidLocalAccount
	}
	event := AuditEvent{
		EventID: attempt.OperationID, OperationID: attempt.OperationID, OccurredAt: attempt.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.local_account.create",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local account creation reservation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local account creation reservation: %w", err))
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
		event.Outcome, event.ResourceKind, event.ResourceID = AuditAttempted, "local_account", attempt.AccountID
		cutoff := attempt.OccurredAt.Add(-localAccountCreationWindow).UTC().UnixNano()
		if _, err := tx.ExecContext(ctx, "DELETE FROM local_account_creation_attempts WHERE occurred_at_ns <= ?", cutoff); err != nil {
			return fmt.Errorf("expire local account creation attempts: %w", err)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO local_account_creation_attempts
(operation_id, actor_id, session_id, account_id, occurred_at_ns, completed) VALUES (?, ?, ?, ?, ?, 0)`,
			attempt.OperationID, attempt.Actor.AccountID, attempt.Actor.SessionID, attempt.AccountID, attempt.OccurredAt.UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("reserve local account creation: %w", err)
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit local account creation reservation: %w", err)
	}
	return decisionErr
}
