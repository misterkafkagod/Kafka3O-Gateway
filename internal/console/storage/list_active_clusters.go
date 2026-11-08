package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// ListActiveClusters returns active registered clusters ordered by ID, including their Gateway URLs for the
// Console's own forwarding; callers decide what reaches the browser.
func (d *Database) ListActiveClusters(ctx context.Context) ([]identity.RegisteredClusterSummary, error) {
	rows, err := d.db.QueryContext(ctx, `SELECT id, display_name, gateway_url, environment FROM registered_clusters
WHERE active = 1 ORDER BY id ASC LIMIT 1001`)
	if err != nil {
		return nil, fmt.Errorf("query active clusters: %w", err)
	}
	clusters := []identity.RegisteredClusterSummary{}
	for rows.Next() {
		cluster := identity.RegisteredClusterSummary{Active: true}
		if err := rows.Scan(&cluster.ID, &cluster.Name, &cluster.GatewayURL, &cluster.Environment); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read active cluster: %w", err)
		}
		clusters = append(clusters, cluster)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("iterate active clusters: %w", err)
	}
	if len(clusters) > 1000 {
		return nil, identity.ErrInvalidClusterRegistry
	}
	return clusters, nil
}
