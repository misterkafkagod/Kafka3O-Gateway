package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) RecordCognitoRefreshFailure(ctx context.Context, failure cognito.RefreshFailure) error {
	return d.recordCognitoRefreshFailureWithBrowserBinding(ctx, failure, [32]byte{})
}
