package main

import (
	"context"
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
	maintainCleanup(ctx, cleaner.CleanupCognitoCredentials, random, now, ticks, logger, cleanupSchedule{
		name: "Cognito credential", items: "Cognito credentials",
		batchSize: storage.CognitoCleanupBatchSize, unknown: storage.ErrCognitoCleanupUnknown,
	})
}
