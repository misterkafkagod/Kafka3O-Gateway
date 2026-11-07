package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func (d *Database) CompleteCognitoRefresh(ctx context.Context, request cognito.RefreshCompletionRequest) error {
	return d.completeCognitoRefreshWithBrowserBinding(ctx, request, [32]byte{})
}
