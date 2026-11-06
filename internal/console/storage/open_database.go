package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

func OpenDatabase(ctx context.Context, path string) (*Database, error) {
	if err := ensureDatabaseFile(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("connect to SQLite: %w", err), db.Close())
	}
	var journal string
	var synchronous, foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return nil, errors.Join(fmt.Errorf("inspect journal mode: %w", err), db.Close())
	}
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return nil, errors.Join(fmt.Errorf("inspect SQLite synchronization: %w", err), db.Close())
	}
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return nil, errors.Join(fmt.Errorf("inspect SQLite foreign keys: %w", err), db.Close())
	}
	if !strings.EqualFold(journal, "wal") || synchronous != 2 || foreignKeys != 1 {
		return nil, errors.Join(ErrDatabaseSettings, db.Close())
	}
	if err := migrateDatabase(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Database{db: db, path: path}, nil
}
