package storage

import (
	"context"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) RevokeBoundSession(ctx context.Context, tokenHash, browserHash [32]byte, eventID string, now time.Time) error {
	if browserHash == ([32]byte{}) {
		return session.ErrInvalidSession
	}
	return d.revokeSessionWithBrowserBinding(ctx, tokenHash, browserHash, eventID, now)
}
