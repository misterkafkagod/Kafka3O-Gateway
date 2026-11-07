package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) RecordBoundCognitoRefreshFailure(ctx context.Context, failure cognito.RefreshFailure, browserHash [32]byte) error {
	if browserHash == ([32]byte{}) {
		return cognito.ErrRefreshUnavailable
	}
	return d.recordCognitoRefreshFailureWithBrowserBinding(ctx, failure, browserHash)
}
