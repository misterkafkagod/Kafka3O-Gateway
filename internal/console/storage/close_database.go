package storage

import "errors"

func (d *Database) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = errors.Join(d.db.Close(), d.lock.Close())
	})
	return d.closeErr
}
