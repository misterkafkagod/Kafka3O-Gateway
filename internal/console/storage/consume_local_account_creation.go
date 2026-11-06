package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func consumeLocalAccountCreation(ctx context.Context, tx *sql.Tx, actor identity.LocalAdminActor, accountID, operationID string, at time.Time) error {
	var actorID, sessionID, reservedAccountID string
	var occurredAt int64
	var completed bool
	err := tx.QueryRowContext(ctx, `SELECT actor_id, session_id, account_id, occurred_at_ns, completed
FROM local_account_creation_attempts WHERE operation_id = ?`, operationID).
		Scan(&actorID, &sessionID, &reservedAccountID, &occurredAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return identity.ErrInvalidAccountCreation
	}
	if err != nil {
		return fmt.Errorf("inspect local account creation reservation: %w", err)
	}
	if completed || actorID != actor.AccountID || sessionID != actor.SessionID || reservedAccountID != accountID {
		return identity.ErrInvalidAccountCreation
	}
	result, err := tx.ExecContext(ctx, `UPDATE local_account_creation_attempts SET completed = 1
WHERE operation_id = ? AND completed = 0`, operationID)
	if err != nil {
		return fmt.Errorf("consume local account creation reservation: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect local account creation reservation consumption: %w", err)
	}
	if count != 1 {
		return identity.ErrInvalidAccountCreation
	}
	now := at.UTC().UnixNano()
	if now < occurredAt || now-occurredAt >= int64(localAccountCreationWindow) {
		return identity.ErrAccountCreationExpired
	}
	return nil
}
