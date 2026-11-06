package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

const (
	localAuthenticationWindow         = 15 * time.Minute
	localAuthenticationPrincipalLimit = 10
	localAuthenticationClientLimit    = 60
	localAuthenticationGlobalLimit    = 300
)

func (d *Database) ReserveLocalAuthentication(ctx context.Context, attempt identity.LocalAuthenticationAttempt) (result error) {
	event := AuditEvent{
		EventID: attempt.EventID, OperationID: attempt.EventID, OccurredAt: attempt.OccurredAt, ActorID: "anonymous",
		Source: AuditSourceAnonymous, Action: "console.local_authentication", Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin local authentication attempt: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back local authentication attempt: %w", err))
		}
	}()
	cutoff := attempt.OccurredAt.Add(-localAuthenticationWindow).UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, "DELETE FROM local_auth_attempts WHERE occurred_at_ns <= ?", cutoff); err != nil {
		return fmt.Errorf("expire local authentication attempts: %w", err)
	}
	var total, principal, client int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(CASE WHEN principal_key = ? THEN 1 ELSE 0 END), 0),
COALESCE(SUM(CASE WHEN client_key = ? THEN 1 ELSE 0 END), 0)
FROM local_auth_attempts`, attempt.PrincipalKey[:], attempt.ClientKey[:]).Scan(&total, &principal, &client)
	if err != nil {
		return fmt.Errorf("inspect local authentication limits: %w", err)
	}
	throttled := principal >= localAuthenticationPrincipalLimit || client >= localAuthenticationClientLimit || total >= localAuthenticationGlobalLimit
	if throttled {
		event.Decision, event.Outcome = "denied", AuditDenied
	} else {
		_, err = tx.ExecContext(ctx, "INSERT INTO local_auth_attempts (principal_key, client_key, occurred_at_ns) VALUES (?, ?, ?)",
			attempt.PrincipalKey[:], attempt.ClientKey[:], attempt.OccurredAt.UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("reserve local authentication attempt: %w", err)
		}
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit local authentication attempt: %w", err)
	}
	if throttled {
		return identity.ErrAuthenticationThrottled
	}
	return nil
}
