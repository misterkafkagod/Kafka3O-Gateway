package storage

import "time"

func (d *Database) transactionTime(requested time.Time) (time.Time, error) {
	now := d.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Year() > 2261 {
		return time.Time{}, ErrInvalidAuditEvent
	}
	// Authorization must not use a time earlier than the request or the transaction.
	if requested.After(now) {
		return requested.UTC(), nil
	}
	return now, nil
}
