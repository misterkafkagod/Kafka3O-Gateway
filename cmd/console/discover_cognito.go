package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
)

// discoverCognito retries provider discovery until it succeeds or ctx ends.
// Local login is served independently while it runs.
func discoverCognito(ctx context.Context, build func(context.Context) (consoleapi.CognitoServices, error),
	publish func(consoleapi.CognitoServices) error, retry func(int) <-chan time.Time, logger *slog.Logger) {
	for attempt := 0; ; attempt++ {
		attemptContext, cancel := context.WithTimeout(ctx, 30*time.Second)
		services, err := build(attemptContext)
		cancel()
		if err == nil {
			if err := publish(services); err != nil {
				logger.Error("Cognito sign-in could not be enabled", "error_type", fmt.Sprintf("%T", err))
				return
			}
			logger.Info("Cognito sign-in available", "attempts", attempt+1)
			return
		}
		if ctx.Err() != nil {
			return
		}
		logger.Warn("Cognito sign-in unavailable", "attempt", attempt+1,
			"provider_mismatch", errors.Is(err, cognito.ErrInvalidProvider), "error_type", fmt.Sprintf("%T", err))
		select {
		case <-ctx.Done():
			return
		case <-retry(attempt):
		}
	}
}
