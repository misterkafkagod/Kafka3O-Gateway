package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) ResolveBoundSessionGrants(ctx context.Context, tokenHash, browserHash [32]byte, clusterID string, requested time.Time,
	protector *cognito.RefreshTokenProtector) ([]authorization.Grant, error) {
	if browserHash == ([32]byte{}) {
		return nil, session.ErrInvalidSession
	}
	return d.resolveSessionGrantsWithBrowserBinding(ctx, tokenHash, browserHash, clusterID, requested, protector)
}
