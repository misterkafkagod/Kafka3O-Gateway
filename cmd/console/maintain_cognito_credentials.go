package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type cognitoCredentialCleaner interface {
	CleanupCognitoCredentials(context.Context, string, time.Time) (int, error)
}

func maintainCognitoCredentials(ctx context.Context, cleaner cognitoCredentialCleaner, random io.Reader,
	now func() time.Time, ticks <-chan time.Time, logger *slog.Logger) {
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
			count, err = cleaner.CleanupCognitoCredentials(work, operation, now().UTC())
			cancel()
			if err == nil && count > 0 {
				logger.Info("Inactive Cognito credentials removed", "operation_id", operation, "count", count)
				if count == storage.CognitoCleanupBatchSize {
					logger.Warn("Cognito credential cleanup batch limit reached; more may remain")
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) && !errors.Is(err, storage.ErrCognitoCleanupUnknown) {
				logger.Info("Cognito credential cleanup interrupted by shutdown", "operation_id", operation, "error_type", fmt.Sprintf("%T", err))
			} else {
				logger.Error("Cognito credential cleanup unavailable", "operation_id", operation, "error_type", fmt.Sprintf("%T", err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				logger.Error("Cognito credential cleanup scheduler stopped")
				return
			}
		}
	}
}
