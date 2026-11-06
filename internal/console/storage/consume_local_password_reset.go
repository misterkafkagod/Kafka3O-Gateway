package storage

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func consumeLocalPasswordReset(ctx context.Context, tx *sql.Tx, actor identity.LocalAdminActor, targetID, operationID string, at time.Time) (int64, error) {
	var actorKey, targetKey []byte
	var sessionID string
	var version sql.NullInt64
	var occurredAt int64
	var completed bool
	err := tx.QueryRowContext(ctx, `SELECT actor_key, target_key, session_id, credential_version, occurred_at_ns, completed
FROM password_reset_attempts WHERE operation_id = ?`, operationID).Scan(&actorKey, &targetKey, &sessionID, &version, &occurredAt, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, identity.ErrInvalidPasswordReset
	}
	if err != nil {
		return 0, fmt.Errorf("inspect password reset reservation: %w", err)
	}
	expectedActor := sha256.Sum256([]byte("actor:" + actor.AccountID))
	expectedTarget := sha256.Sum256([]byte("target:" + targetID))
	if completed || !version.Valid || sessionID != actor.SessionID || subtle.ConstantTimeCompare(actorKey, expectedActor[:]) != 1 ||
		subtle.ConstantTimeCompare(targetKey, expectedTarget[:]) != 1 {
		return 0, identity.ErrInvalidPasswordReset
	}
	result, err := tx.ExecContext(ctx, "UPDATE password_reset_attempts SET completed = 1 WHERE operation_id = ? AND completed = 0", operationID)
	if err != nil {
		return 0, fmt.Errorf("consume password reset reservation: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect password reset reservation consumption: %w", err)
	}
	if count != 1 {
		return 0, identity.ErrInvalidPasswordReset
	}
	now := at.UTC().UnixNano()
	if now < occurredAt || now-occurredAt >= int64(passwordResetWindow) {
		return version.Int64, identity.ErrPasswordResetExpired
	}
	return version.Int64, nil
}
