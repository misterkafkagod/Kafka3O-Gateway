package storage

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

func openDatabaseLock(path string) (*os.File, error) {
	lockPath := path + ".lock"
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open Console database lock: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect Console database lock: %w", err), file.Close())
	}
	pathInfo, err := os.Lstat(lockPath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect Console database lock path: %w", err), file.Close())
	}
	if !info.Mode().IsRegular() || !pathInfo.Mode().IsRegular() || !os.SameFile(info, pathInfo) ||
		(runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return nil, errors.Join(ErrInvalidDatabaseLock, file.Close())
	}
	if err := lockDatabaseFile(file); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
