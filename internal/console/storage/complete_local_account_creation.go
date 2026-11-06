package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) CompleteLocalAccountCreation(ctx context.Context, completion identity.LocalAccountCreationCompletion) (result error) {
	account := completion.Account
	name, err := identity.NormalizeLocalUsername(account.Username)
	if err != nil || name != account.Username {
		return identity.ErrInvalidUsername
	}
	if strings.TrimSpace(account.ID) == "" || len(account.ID) > 128 || !account.Enabled ||
		!account.MustChangePassword || account.CredentialVersion != 1 {
		return identity.ErrInvalidLocalAccount
	}
	if err := identity.ValidateLocalPasswordHash(account.PasswordHash); err != nil {
		return err
	}
	event := AuditEvent{
		EventID: completion.EventID, OperationID: completion.OperationID, OccurredAt: completion.OccurredAt,
		ActorID: completion.Actor.AccountID, Source: AuditSourceLocal, Action: "console.local_account.create",
		ResourceKind: "local_account", ResourceID: account.ID, Decision: "allowed", Outcome: AuditSucceeded,
	}
	if err := validateAuditEvent(event); err != nil || completion.OperationID == "" || completion.EventID == completion.OperationID {
		return ErrInvalidAuditEvent
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local account creation: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local account creation: %w", err))
		}
	}()
	completion.OccurredAt, err = d.transactionTime(completion.OccurredAt)
	if err != nil {
		return err
	}
	event.OccurredAt = completion.OccurredAt
	consumeErr := consumeLocalAccountCreation(ctx, tx, completion.Actor, account.ID, completion.OperationID, completion.OccurredAt)
	var decisionErr error
	switch {
	case errors.Is(consumeErr, identity.ErrAccountCreationExpired):
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
			var exists bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM local_accounts WHERE username = ?)", account.Username).Scan(&exists); err != nil {
				return fmt.Errorf("inspect local username availability: %w", err)
			}
			if exists {
				event.Outcome, decisionErr = AuditValidationRejected, identity.ErrUsernameTaken
				break
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES (?, 1)", account.ID); err != nil {
				return fmt.Errorf("create local identity: %w", err)
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO local_accounts
(identity_id, username, password_hash, must_change_password, credential_version) VALUES (?, ?, ?, 1, 1)`,
				account.ID, account.Username, account.PasswordHash)
			if err != nil {
				return fmt.Errorf("create local account: %w", err)
			}
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(identity.ErrAccountCreationUnknown, fmt.Errorf("commit local account creation: %w", err))
	}
	return decisionErr
}
