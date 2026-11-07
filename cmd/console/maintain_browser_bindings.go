package main

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type browserBindingCleaner interface {
	CleanupBrowserBindings(context.Context, string, time.Time) (int, error)
}

func maintainBrowserBindings(ctx context.Context, cleaner browserBindingCleaner, random io.Reader,
	now func() time.Time, ticks <-chan time.Time, logger *slog.Logger) {
	maintainCleanup(ctx, cleaner.CleanupBrowserBindings, random, now, ticks, logger, cleanupSchedule{
		name: "Browser binding", items: "browser bindings",
		batchSize: storage.BrowserBindingCleanupBatchSize, unknown: storage.ErrBrowserBindingCleanupUnknown,
	})
}
