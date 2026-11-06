package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) UseSession(ctx context.Context, tokenHash [32]byte, clock func() time.Time, idleLifetime time.Duration,
	protector *cognito.RefreshTokenProtector) (session.Principal, error) {
	return d.useSessionWithBrowserBinding(ctx, tokenHash, [32]byte{}, clock, idleLifetime, protector)
}
