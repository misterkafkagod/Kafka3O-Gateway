package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// backupRetention is how many SQLite snapshots are kept in the backup directory.
const backupRetention = 7

// maintainBackups writes a SQLite-consistent snapshot now and on every tick, keeping the newest backupRetention
// snapshots. Failures are logged as alerts and never stop the Console.
func maintainBackups(ctx context.Context, backup func(context.Context, string) error, directory string, now func() time.Time,
	ticks <-chan time.Time, logger *slog.Logger) {
	for {
		name := filepath.Join(directory, "console-"+now().UTC().Format("20060102T150405.000000000Z")+".sqlite")
		work, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := backup(work, name)
		cancel()
		if err != nil {
			logger.Error("Console backup failed", "error_type", fmt.Sprintf("%T", err))
		} else {
			logger.Info("Console backup written", "file", filepath.Base(name))
			entries, err := os.ReadDir(directory)
			if err == nil {
				var snapshots []string
				for _, entry := range entries {
					if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "console-") && strings.HasSuffix(entry.Name(), ".sqlite") {
						snapshots = append(snapshots, entry.Name())
					}
				}
				sort.Strings(snapshots)
				for len(snapshots) > backupRetention {
					if err := os.Remove(filepath.Join(directory, snapshots[0])); err != nil {
						logger.Error("old Console backup could not be removed", "error_type", fmt.Sprintf("%T", err))
						break
					}
					snapshots = snapshots[1:]
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}
	}
}
