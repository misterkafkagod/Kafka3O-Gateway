package storage

import (
	"context"
)

func (d *Database) AppendAuditEvent(ctx context.Context, event AuditEvent) error {
	return insertAuditEvent(ctx, d.db, event)
}
