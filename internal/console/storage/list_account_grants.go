package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func (d *Database) ListAccountGrants(ctx context.Context, accountID string) ([]authorization.Grant, error) {
	if strings.TrimSpace(accountID) == "" {
		return nil, identity.ErrAccountNotFound
	}
	rows, err := d.db.QueryContext(ctx, "SELECT cluster_id, role FROM role_grants WHERE account_id = ? ORDER BY cluster_id", accountID)
	if err != nil {
		return nil, fmt.Errorf("query account grants: %w", err)
	}
	defer rows.Close()
	grants := make([]authorization.Grant, 0)
	for rows.Next() {
		var grant authorization.Grant
		if err := rows.Scan(&grant.ClusterID, &grant.Role); err != nil {
			return nil, fmt.Errorf("read account grant: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate account grants: %w", err)
	}
	return grants, nil
}
