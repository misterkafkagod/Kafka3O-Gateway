package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) InspectCognitoRefresh(ctx context.Context, tokenHash [32]byte, requested time.Time) (session.CognitoRefreshState, error) {
	return d.inspectCognitoRefreshWithBrowserBinding(ctx, tokenHash, [32]byte{}, requested)
}
