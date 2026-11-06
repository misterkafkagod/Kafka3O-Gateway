package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) CreateInitialAdmin(ctx context.Context, account identity.LocalAccount, eventID string, occurredAt time.Time) (result error) {
	name, err := identity.NormalizeLocalUsername(account.Username)
	if err != nil || name != account.Username {
		return identity.ErrInvalidUsername
	}
	if strings.TrimSpace(account.ID) == "" || len(account.ID) > 128 || !account.Enabled || !account.MustChangePassword {
		return identity.ErrInvalidLocalAccount
	}
	if err := identity.ValidateLocalPasswordHash(account.PasswordHash); err != nil {
		return err
	}
	event := AuditEvent{
		EventID: eventID, OccurredAt: occurredAt, ActorID: account.ID, Source: AuditSourceLocal,
		Action: "console.bootstrap", Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin initial administrator creation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back initial administrator creation: %w", err))
		}
	}()
	res, err := tx.ExecContext(ctx, "UPDATE bootstrap_state SET completed = 1 WHERE id = 1 AND completed = 0")
	if err != nil {
		return fmt.Errorf("consume bootstrap state: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect bootstrap state: %w", err)
	}
	if count != 1 {
		return identity.ErrAlreadyBootstrapped
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES (?, ?)", account.ID, account.Enabled); err != nil {
		return fmt.Errorf("create initial identity: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO local_accounts
(identity_id, username, password_hash, must_change_password) VALUES (?, ?, ?, ?)`,
		account.ID, account.Username, account.PasswordHash, account.MustChangePassword)
	if err != nil {
		return fmt.Errorf("create initial administrator: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, ?, ?)",
		account.ID, "", authorization.RoleAdmin); err != nil {
		return fmt.Errorf("grant initial administrator role: %w", err)
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit initial administrator: %w", err)
	}
	return nil
}
