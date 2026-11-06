package storage

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) FindCognitoSessionForLogout(ctx context.Context, tokenHash [32]byte) (session.Principal, error) {
	return d.findCognitoSessionForLogoutWithBrowserBinding(ctx, tokenHash, [32]byte{})
}
