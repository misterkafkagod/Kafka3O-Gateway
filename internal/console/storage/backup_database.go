package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (d *Database) Backup(ctx context.Context, destination string) error {
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || destination == d.path {
		return ErrInvalidDatabasePath
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("create SQLite backup: %w", err)
	}
	if err := file.Close(); err != nil {
		removeErr := os.Remove(destination)
		return errors.Join(fmt.Errorf("close new SQLite backup: %w", err), removeErr)
	}
	if _, err := d.db.ExecContext(ctx, "VACUUM INTO ?", destination); err != nil {
		removeErr := os.Remove(destination)
		return errors.Join(fmt.Errorf("create SQLite snapshot: %w", err), removeErr)
	}
	file, err = os.OpenFile(destination, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open SQLite snapshot for sync: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil || closeErr != nil {
		return errors.Join(syncErr, closeErr)
	}
	return nil
}
