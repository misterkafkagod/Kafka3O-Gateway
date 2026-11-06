//go:build !windows && !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package storage

import "os"

func lockDatabaseFile(_ *os.File) error {
	return ErrUnsupportedLock
}
