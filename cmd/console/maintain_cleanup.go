package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

type cleanupSchedule struct {
	name      string
	items     string
	batchSize int
	unknown   error
}

func maintainCleanup(ctx context.Context, cleanup func(context.Context, string, time.Time) (int, error),
	random io.Reader, now func() time.Time, ticks <-chan time.Time, logger *slog.Logger, schedule cleanupSchedule) {
	for {
		if ctx.Err() != nil {
			return
		}
		var randomID [16]byte
		var operation string
		_, err := io.ReadFull(random, randomID[:])
		if err == nil {
			operation = base64.RawURLEncoding.EncodeToString(randomID[:])
			work, cancel := context.WithTimeout(ctx, 10*time.Second)
			var count int
			count, err = cleanup(work, operation, now().UTC())
			cancel()
			if err == nil && count > 0 {
				logger.Info("Inactive "+schedule.items+" removed", "operation_id", operation, "count", count)
				if count == schedule.batchSize {
					logger.Warn(schedule.name + " cleanup batch limit reached; more may remain")
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) && !errors.Is(err, schedule.unknown) {
				logger.Info(schedule.name+" cleanup interrupted by shutdown", "operation_id", operation, "error_type", fmt.Sprintf("%T", err))
			} else {
				logger.Error(schedule.name+" cleanup unavailable", "operation_id", operation, "error_type", fmt.Sprintf("%T", err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				logger.Error(schedule.name + " cleanup scheduler stopped")
				return
			}
		}
	}
}
