package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) FinishLocalAuthentication(ctx context.Context, authentication identity.LocalAuthenticationResult) (authenticated bool, result error) {
	event := AuditEvent{
		EventID: authentication.EventID, OperationID: authentication.OperationID,
		OccurredAt: authentication.OccurredAt, ActorID: "anonymous",
		Source: AuditSourceAnonymous, Action: "console.local_authentication", Decision: "denied", Outcome: AuditFailed,
	}
	if err := validateAuditEvent(event); err != nil {
		return false, err
	}
	if authentication.OperationID == "" || (authentication.AccountID == "") != (authentication.PasswordHash == "") {
		return false, identity.ErrInvalidLocalAccount
	}
	if authentication.AccountID != "" {
		if len(authentication.AccountID) > 128 {
			return false, identity.ErrInvalidLocalAccount
		}
		if err := identity.ValidateLocalPasswordHash(authentication.PasswordHash); err != nil {
			return false, err
		}
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin local authentication completion: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local authentication completion: %w", err))
		}
	}()
	if authentication.AccountID != "" {
		err = tx.QueryRowContext(ctx, `SELECT EXISTS (
SELECT 1 FROM local_accounts JOIN identities ON identities.id = local_accounts.identity_id
WHERE identities.id = ? AND identities.enabled = 1 AND local_accounts.password_hash = ?
)`, authentication.AccountID, authentication.PasswordHash).Scan(&authenticated)
		if err != nil {
			return false, fmt.Errorf("recheck local identity: %w", err)
		}
	}
	if authenticated {
		event.ActorID, event.Source, event.Decision, event.Outcome =
			authentication.AccountID, AuditSourceLocal, "allowed", AuditSucceeded
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit local authentication completion: %w", err)
	}
	return authenticated, nil
}
