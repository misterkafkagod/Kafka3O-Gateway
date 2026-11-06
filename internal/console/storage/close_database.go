package storage

func (d *Database) Close() error {
	return d.db.Close()
}
