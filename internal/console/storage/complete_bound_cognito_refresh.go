package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) CompleteBoundCognitoRefresh(ctx context.Context, request cognito.RefreshCompletionRequest, browserHash [32]byte) error {
	if browserHash == ([32]byte{}) {
		return cognito.ErrRefreshUnavailable
	}
	return d.completeCognitoRefreshWithBrowserBinding(ctx, request, browserHash)
}
