package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) CreateCognitoAttempt(ctx context.Context, attempt cognito.AttemptRecord) (result error) {
	if attempt.StateHash == ([32]byte{}) || attempt.BindingHash == ([32]byte{}) ||
		attempt.NonceHash == ([32]byte{}) || len(attempt.EncryptedPKCE) != cognito.EncryptedVerifierSize ||
		!attempt.ExpiresAt.Equal(attempt.OccurredAt.Add(cognito.AttemptLifetime)) ||
		attempt.ExpiresAt.Year() > 2261 {
		return cognito.ErrInvalidAttemptConfig
	}
	event := AuditEvent{
		EventID: attempt.OperationID, OperationID: attempt.OperationID, OccurredAt: attempt.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.authorize",
		Decision: "allowed", Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Cognito authorization attempt: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back Cognito authorization attempt: %w", err))
		}
	}()
	now, err := d.transactionTime(attempt.OccurredAt)
	if err != nil {
		return err
	}
	event.OccurredAt = now
	if _, err := tx.ExecContext(ctx, "DELETE FROM cognito_authorization_attempts WHERE expires_at_ns <= ?", now.UnixNano()); err != nil {
		return fmt.Errorf("expire Cognito authorization attempts: %w", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil {
		return fmt.Errorf("inspect Cognito authorization attempt limit: %w", err)
	}
	var denied error
	switch {
	case !attempt.ExpiresAt.After(now):
		denied = cognito.ErrInvalidAttempt
	case count >= cognito.MaxActiveAttempts:
		denied = cognito.ErrAttemptLimit
	default:
		_, err := tx.ExecContext(ctx, `INSERT INTO cognito_authorization_attempts
(operation_id, state_hash, binding_hash, nonce_hash, encrypted_pkce, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?)`, attempt.OperationID, attempt.StateHash[:], attempt.BindingHash[:],
			attempt.NonceHash[:], attempt.EncryptedPKCE, now.UnixNano(), attempt.ExpiresAt.UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("persist Cognito authorization attempt: %w", err)
		}
	}
	if denied != nil {
		event.Decision, event.Outcome = "denied", AuditDenied
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(cognito.ErrAttemptUnknown, fmt.Errorf("commit Cognito authorization attempt: %w", err))
	}
	return denied
}
