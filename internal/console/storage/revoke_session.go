package storage

import (
	"context"
	"time"
)

func (d *Database) RevokeSession(ctx context.Context, tokenHash [32]byte, eventID string, now time.Time) error {
	return d.revokeSessionWithBrowserBinding(ctx, tokenHash, [32]byte{}, eventID, now)
}
