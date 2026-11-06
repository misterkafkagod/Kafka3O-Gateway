package storage

import (
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (d *Database) validateBrowserSessionLifetime(record session.Record, requested time.Time) (time.Time, error) {
	if record.BrowserBindingHash == ([32]byte{}) {
		return requested, nil
	}
	now := d.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Year() > 2261 || now.Before(requested) {
		return time.Time{}, ErrInvalidAuditEvent
	}
	if !record.IdleExpiresAt.After(now) || !record.AbsoluteExpiresAt.After(now) {
		return now, session.ErrInvalidSession
	}
	return now, nil
}
