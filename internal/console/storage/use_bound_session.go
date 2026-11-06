package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) UseBoundSession(ctx context.Context, tokenHash, browserHash [32]byte, clock func() time.Time, idleLifetime time.Duration,
	protector *cognito.RefreshTokenProtector) (session.Principal, error) {
	if browserHash == ([32]byte{}) {
		return session.Principal{}, session.ErrInvalidSession
	}
	return d.useSessionWithBrowserBinding(ctx, tokenHash, browserHash, clock, idleLifetime, protector)
}
