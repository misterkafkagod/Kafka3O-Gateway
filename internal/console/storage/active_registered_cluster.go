package storage

import (
	"context"
	"fmt"
)

// ActiveRegisteredCluster reports whether a cluster ID belongs to an active registered cluster.
func (d *Database) ActiveRegisteredCluster(ctx context.Context, clusterID string) (bool, error) {
	var active bool
	if err := d.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM registered_clusters WHERE id = ? AND active = 1)", clusterID).Scan(&active); err != nil {
		return false, fmt.Errorf("inspect registered cluster: %w", err)
	}
	return active, nil
}
