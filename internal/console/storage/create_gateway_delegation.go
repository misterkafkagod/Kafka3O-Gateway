package storage

import (
	"context"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
)

// CreateGatewayDelegation stores a delegation digest and removes delegations that expired unused.
func (d *Database) CreateGatewayDelegation(ctx context.Context, record delegation.Record) error {
	if record.HandleHash == ([32]byte{}) || record.Binding == ([32]byte{}) || record.SessionID == "" || record.ClusterID == "" ||
		record.Action == "" || record.Resource == "" || len(record.Resource) > 512 || len(record.Reason) > 512 ||
		!record.CreatedAt.Before(record.ExpiresAt) || record.ExpiresAt.Sub(record.CreatedAt) > delegation.Lifetime {
		return delegation.ErrInvalidMint
	}
	if _, err := d.db.ExecContext(ctx, "DELETE FROM gateway_delegations WHERE expires_at_ns <= ?", record.CreatedAt.UTC().UnixNano()); err != nil {
		return fmt.Errorf("remove expired gateway delegations: %w", err)
	}
	if _, err := d.db.ExecContext(ctx, `INSERT INTO gateway_delegations
(handle_hash, session_id, cluster_id, action, resource, binding, reason, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, record.HandleHash[:], record.SessionID, record.ClusterID, record.Action, record.Resource,
		record.Binding[:], record.Reason, record.CreatedAt.UTC().UnixNano(), record.ExpiresAt.UTC().UnixNano()); err != nil {
		return fmt.Errorf("store gateway delegation: %w", err)
	}
	return nil
}
