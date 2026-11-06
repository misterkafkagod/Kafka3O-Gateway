package storage

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func lockDatabaseFile(file *os.File) error {
	var overlapped windows.Overlapped
	err := windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return ErrDatabaseInUse
	}
	if err != nil {
		return fmt.Errorf("lock Console database: %w", err)
	}
	return nil
}
