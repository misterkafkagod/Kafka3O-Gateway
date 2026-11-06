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
	bootstrapAttemptWindow = 15 * time.Minute
	bootstrapAttemptLimit  = 10
)

func (d *Database) ReserveBootstrapAttempt(ctx context.Context, attempt identity.BootstrapAttempt) (result error) {
	event := AuditEvent{
		EventID: attempt.EventID, OperationID: attempt.EventID, OccurredAt: attempt.OccurredAt,
		ActorID: "anonymous", Source: AuditSourceAnonymous, Action: "console.bootstrap", Outcome: AuditAttempted,
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap attempt: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back bootstrap attempt: %w", err))
		}
	}()
	var completed bool
	if err := tx.QueryRowContext(ctx, "SELECT completed FROM bootstrap_state WHERE id = 1").Scan(&completed); err != nil {
		return fmt.Errorf("inspect bootstrap state: %w", err)
	}
	cutoff := attempt.OccurredAt.Add(-bootstrapAttemptWindow).UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, "DELETE FROM bootstrap_attempts WHERE occurred_at_ns <= ?", cutoff); err != nil {
		return fmt.Errorf("expire bootstrap attempts: %w", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM bootstrap_attempts").Scan(&count); err != nil {
		return fmt.Errorf("inspect bootstrap attempt limit: %w", err)
	}
	var denied error
	switch {
	case completed:
		denied = identity.ErrAlreadyBootstrapped
	case count >= bootstrapAttemptLimit:
		denied = identity.ErrBootstrapThrottled
	}
	if denied != nil {
		event.Decision, event.Outcome = "denied", AuditDenied
	} else if _, err := tx.ExecContext(ctx, "INSERT INTO bootstrap_attempts (occurred_at_ns) VALUES (?)",
		attempt.OccurredAt.UTC().UnixNano()); err != nil {
		return fmt.Errorf("reserve bootstrap attempt: %w", err)
	}
	if err := insertAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap attempt: %w", err)
	}
	return denied
}
