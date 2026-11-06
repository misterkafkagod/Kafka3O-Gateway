package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func ensureDatabaseFile(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrInvalidDatabasePath
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		file, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if createErr != nil {
			return fmt.Errorf("create database file: %w", createErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("close new database file: %w", closeErr)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect database file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return ErrInvalidDatabasePath
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return ErrInsecureDatabase
	}
	return nil
}
