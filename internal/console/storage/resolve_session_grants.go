package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) ResolveSessionGrants(ctx context.Context, tokenHash [32]byte, clusterID string, requested time.Time,
	protector *cognito.RefreshTokenProtector) ([]authorization.Grant, error) {
	return d.resolveSessionGrantsWithBrowserBinding(ctx, tokenHash, [32]byte{}, clusterID, requested, protector)
}
