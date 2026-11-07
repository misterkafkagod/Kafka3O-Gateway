package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ConsumeCognitoAttempt(ctx context.Context, request cognito.AttemptConsumption) (record cognito.AttemptRecord, result error) {
	event := AuditEvent{
		EventID: request.EventID, OperationID: request.EventID, OccurredAt: request.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.cognito.callback",
		Decision: "denied", Outcome: AuditDenied,
	}
	if err := validateAuditEvent(event); err != nil {
		return cognito.AttemptRecord{}, err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return cognito.AttemptRecord{}, fmt.Errorf("begin Cognito callback consumption: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back Cognito callback consumption: %w", err))
		}
	}()
	transactionTime := d.transactionTime
	if request.Bound {
		transactionTime = d.strictTransactionTime
	}
	now, err := transactionTime(request.OccurredAt)
	if err != nil {
		return cognito.AttemptRecord{}, err
	}
	event.OccurredAt = now
	decision := cognito.ErrInvalidAttempt
	record.OperationID = request.EventID
	if request.Valid && request.StateHash != ([32]byte{}) && request.BindingHash != ([32]byte{}) {
		var nonceHash []byte
		var expiresAt int64
		err = tx.QueryRowContext(ctx, `DELETE FROM cognito_authorization_attempts
WHERE state_hash = ? AND binding_hash = ? AND created_at_ns <= ? AND expires_at_ns > ?
RETURNING operation_id, nonce_hash, encrypted_pkce, expires_at_ns`,
			request.StateHash[:], request.BindingHash[:], now.UnixNano(), now.UnixNano()).Scan(
			&record.OperationID, &nonceHash, &record.EncryptedPKCE, &expiresAt)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			record.OperationID = request.EventID
		case err != nil:
			return cognito.AttemptRecord{}, fmt.Errorf("consume Cognito authorization attempt: %w", err)
		default:
			event.OperationID = record.OperationID
			if len(nonceHash) != len(record.NonceHash) || len(record.EncryptedPKCE) != cognito.EncryptedVerifierSize {
				decision = cognito.ErrAttemptEncryption
			} else {
				copy(record.NonceHash[:], nonceHash)
				record.OccurredAt = now
				record.ExpiresAt = time.Unix(0, expiresAt).UTC()
				event.EventID = record.OperationID + ".callback"
				event.Decision, event.Outcome, decision = "allowed", AuditAccepted, nil
			}
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return cognito.AttemptRecord{OperationID: record.OperationID}, err
	}
	if err := tx.Commit(); err != nil {
		return cognito.AttemptRecord{OperationID: record.OperationID},
			errors.Join(cognito.ErrAttemptUnknown, fmt.Errorf("commit Cognito callback consumption: %w", err))
	}
	if decision != nil {
		return cognito.AttemptRecord{OperationID: record.OperationID}, decision
	}
	return record, nil
}
