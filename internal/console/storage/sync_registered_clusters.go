package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

// SyncRegisteredClusters applies the operator's cluster file atomically: new IDs are registered unclassified,
// display names follow the file, absent IDs are deactivated (their classification is kept), and a changed
// Gateway URL is refused so a classification is never silently reused for another Gateway. Every change is
// audited as the Console service under operationID.
func (d *Database) SyncRegisteredClusters(ctx context.Context, clusters []identity.RegisteredCluster, operationID string, at time.Time) (result error) {
	if len(clusters) > 1000 || operationID == "" || len(operationID) > 100 {
		return identity.ErrInvalidClusterRegistry
	}
	seen := make(map[string]struct{}, len(clusters))
	for _, cluster := range clusters {
		if _, duplicate := seen[cluster.ID]; duplicate || identity.ValidateClusterID(cluster.ID) != nil ||
			cluster.Name == "" || len(cluster.Name) > 128 || cluster.GatewayURL == "" || len(cluster.GatewayURL) > 512 {
			return identity.ErrInvalidClusterRegistry
		}
		seen[cluster.ID] = struct{}{}
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cluster registry sync: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, fmt.Errorf("roll back cluster registry sync: %w", err))
		}
	}()
	at, err = d.transactionTime(at)
	if err != nil {
		return err
	}
	sequence := 0
	for _, cluster := range clusters {
		var name, url, environment string
		var active bool
		err := tx.QueryRowContext(ctx, "SELECT display_name, gateway_url, environment, active FROM registered_clusters WHERE id = ?",
			cluster.ID).Scan(&name, &url, &environment, &active)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.ExecContext(ctx, `INSERT INTO registered_clusters (id, display_name, gateway_url, environment, active)
VALUES (?, ?, ?, '', 1)`, cluster.ID, cluster.Name, cluster.GatewayURL); err != nil {
				return fmt.Errorf("register cluster: %w", err)
			}
			sequence++
			if err := insertAuditEvent(ctx, tx, registryAuditEvent(operationID, sequence, at, "console.cluster.register", cluster.ID, "")); err != nil {
				return err
			}
		case err != nil:
			return fmt.Errorf("inspect registered cluster: %w", err)
		case url != cluster.GatewayURL:
			return identity.ErrClusterRetarget
		case name != cluster.Name || !active:
			if _, err := tx.ExecContext(ctx, "UPDATE registered_clusters SET display_name = ?, active = 1 WHERE id = ?",
				cluster.Name, cluster.ID); err != nil {
				return fmt.Errorf("update registered cluster: %w", err)
			}
			action := "console.cluster.rename"
			if !active {
				action = "console.cluster.reactivate"
			}
			sequence++
			if err := insertAuditEvent(ctx, tx, registryAuditEvent(operationID, sequence, at, action, cluster.ID, environment)); err != nil {
				return err
			}
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT id, environment FROM registered_clusters WHERE active = 1 ORDER BY id")
	if err != nil {
		return fmt.Errorf("query active clusters: %w", err)
	}
	type inactive struct{ id, environment string }
	var removed []inactive
	for rows.Next() {
		var entry inactive
		if err := rows.Scan(&entry.id, &entry.environment); err != nil {
			rows.Close()
			return fmt.Errorf("read active cluster: %w", err)
		}
		if _, configured := seen[entry.id]; !configured {
			removed = append(removed, entry)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("iterate active clusters: %w", err)
	}
	for _, entry := range removed {
		if _, err := tx.ExecContext(ctx, "UPDATE registered_clusters SET active = 0 WHERE id = ?", entry.id); err != nil {
			return fmt.Errorf("deactivate registered cluster: %w", err)
		}
		sequence++
		if err := insertAuditEvent(ctx, tx, registryAuditEvent(operationID, sequence, at, "console.cluster.deactivate", entry.id, entry.environment)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cluster registry sync: %w", err)
	}
	return nil
}
