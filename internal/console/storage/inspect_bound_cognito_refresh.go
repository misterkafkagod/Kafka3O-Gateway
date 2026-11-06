package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) InspectBoundCognitoRefresh(ctx context.Context, tokenHash, browserHash [32]byte, requested time.Time) (session.CognitoRefreshState, error) {
	if browserHash == ([32]byte{}) {
		return 0, session.ErrInvalidSession
	}
	return d.inspectCognitoRefreshWithBrowserBinding(ctx, tokenHash, browserHash, requested)
}
