//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package storage

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockDatabaseFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrDatabaseInUse
	}
	if err != nil {
		return fmt.Errorf("lock Console database: %w", err)
	}
	return nil
}
