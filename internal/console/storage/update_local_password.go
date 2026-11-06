package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) UpdateLocalPassword(ctx context.Context, update identity.LocalPasswordUpdate) (result error) {
	if strings.TrimSpace(update.AccountID) == "" || len(update.AccountID) > 128 {
		return identity.ErrInvalidLocalAccount
	}
	if err := identity.ValidateLocalPasswordHash(update.PreviousHash); err != nil {
		return err
	}
	if err := identity.ValidateLocalPasswordHash(update.NewHash); err != nil {
		return err
	}
	event := AuditEvent{
		EventID: update.EventID, OperationID: update.EventID, OccurredAt: update.OccurredAt,
		ActorID: update.AccountID, Source: AuditSourceLocal, Action: "console.local_password_change",
		Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local password update: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local password update: %w", err))
		}
	}()
	res, err := tx.ExecContext(ctx, `UPDATE local_accounts SET password_hash = ?, must_change_password = 0
WHERE identity_id = ? AND password_hash = ? AND EXISTS (
SELECT 1 FROM identities WHERE id = local_accounts.identity_id AND enabled = 1
)`, update.NewHash, update.AccountID, update.PreviousHash)
	if err != nil {
		return fmt.Errorf("update local password: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect local password update: %w", err)
	}
	if count != 1 {
		event.Decision, event.Outcome = "denied", AuditFailed
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit local password update: %w", err)
	}
	if count != 1 {
		return identity.ErrAuthenticationFailed
	}
	return nil
}
