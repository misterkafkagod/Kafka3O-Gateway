package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) FindBoundCognitoSessionForLogout(ctx context.Context, tokenHash, browserHash [32]byte) (session.Principal, error) {
	if browserHash == ([32]byte{}) {
		return session.Principal{}, session.ErrInvalidSession
	}
	return d.findCognitoSessionForLogoutWithBrowserBinding(ctx, tokenHash, browserHash)
}
