package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// ConsumeGatewayDelegation removes the delegation in one transaction, so each handle is used at most once even
// when the caller then denies it, and returns the session it was minted for.
func (d *Database) ConsumeGatewayDelegation(ctx context.Context, handleHash [32]byte, at time.Time) (consumed delegation.Consumed, result error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return delegation.Consumed{}, fmt.Errorf("begin gateway delegation consume: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			consumed = delegation.Consumed{}
			result = errors.Join(result, fmt.Errorf("roll back gateway delegation consume: %w", err))
		}
	}()
	var action string
	var binding, tokenHash, browserHash []byte
	var created, expires int64
	err = tx.QueryRowContext(ctx, `SELECT d.session_id, d.cluster_id, d.action, d.resource, d.binding, d.reason,
d.created_at_ns, d.expires_at_ns, s.token_hash, s.browser_binding_hash, s.identity_id, s.auth_source
FROM gateway_delegations d JOIN console_sessions s ON s.id = d.session_id WHERE d.handle_hash = ?`, handleHash[:]).Scan(
		&consumed.SessionID, &consumed.ClusterID, &action, &consumed.Resource, &binding, &consumed.Reason,
		&created, &expires, &tokenHash, &browserHash, &consumed.AccountID, &consumed.Source)
	if errors.Is(err, sql.ErrNoRows) {
		return delegation.Consumed{}, delegation.ErrInvalidDelegation
	}
	if err != nil {
		return delegation.Consumed{}, fmt.Errorf("read gateway delegation: %w", err)
	}
	if len(binding) != 32 || len(tokenHash) != 32 || (browserHash != nil && len(browserHash) != 32) {
		return delegation.Consumed{}, fmt.Errorf("gateway delegation has invalid stored digests")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM gateway_delegations WHERE handle_hash = ?", handleHash[:]); err != nil {
		return delegation.Consumed{}, fmt.Errorf("consume gateway delegation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return delegation.Consumed{}, fmt.Errorf("commit gateway delegation consume: %w", err)
	}
	consumed.HandleHash, consumed.Action = handleHash, authorization.Action(action)
	consumed.Binding, consumed.TokenHash = [32]byte(binding), [32]byte(tokenHash)
	if browserHash != nil {
		consumed.BrowserHash = [32]byte(browserHash)
	}
	consumed.CreatedAt, consumed.ExpiresAt = time.Unix(0, created).UTC(), time.Unix(0, expires).UTC()
	return consumed, nil
}
