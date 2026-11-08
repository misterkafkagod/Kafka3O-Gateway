package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

// ClusterEnvironment returns the stored classification of an active registered cluster. Unknown, inactive and
// unclassified clusters are all unclassified, which denies Kafka mutations; a storage failure is an error and
// never a non-Production default.
func (d *Database) ClusterEnvironment(ctx context.Context, clusterID string) (authorization.Environment, error) {
	var environment string
	err := d.db.QueryRowContext(ctx, "SELECT environment FROM registered_clusters WHERE id = ? AND active = 1", clusterID).Scan(&environment)
	if errors.Is(err, sql.ErrNoRows) {
		return authorization.EnvironmentUnclassified, nil
	}
	if err != nil {
		return authorization.EnvironmentUnclassified, fmt.Errorf("read cluster environment: %w", err)
	}
	switch value := authorization.Environment(environment); value {
	case authorization.EnvironmentUnclassified, authorization.EnvironmentNonProduction, authorization.EnvironmentProduction:
		return value, nil
	default:
		return authorization.EnvironmentUnclassified, authorization.ErrInvalidEnvironment
	}
}
