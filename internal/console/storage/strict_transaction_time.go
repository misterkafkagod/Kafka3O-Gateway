package storage

import "time"

func (d *Database) strictTransactionTime(requested time.Time) (time.Time, error) {
	now := d.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Year() > 2261 || now.Before(requested) {
		return time.Time{}, ErrInvalidAuditEvent
	}
	return now, nil
}
