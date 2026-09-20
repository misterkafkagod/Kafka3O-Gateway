package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// telemetryShutdowner is the surface runServer needs from
// *telemetry.Providers, narrow enough that a test can supply a fake.
type telemetryShutdowner interface {
	Shutdown(ctx context.Context) error
}

// kafkaCloser is the surface runServer needs from *franz.Client.
type kafkaCloser interface {
	Close()
}

// runServer serves on listener until ctx is cancelled, then shuts down in
// the documented order (TECH-SPEC §5.4): stop accepting new connections →
// drain in-flight requests (bounded by shutdownTimeout) → OTel shutdown →
// close the Kafka client → log "shutdown complete".
func runServer(
	ctx context.Context,
	listener net.Listener,
	server *http.Server,
	shutdownTimeout time.Duration,
	providers telemetryShutdowner,
	kafkaClient kafkaCloser,
	logger *slog.Logger,
) error {
	errCh := make(chan error, 1)
	go func() {
		var err error
		if server.TLSConfig != nil {
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Stop accepting and drain in-flight requests before anything else
	// closes, so no handler observes a shut-down tracer or Kafka client
	// mid-request.
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("app: HTTP shutdown: %w", err)
	}
	<-errCh // wait for the Serve goroutine to actually return

	if err := providers.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("app: telemetry shutdown: %w", err)
	}

	kafkaClient.Close()

	logger.Info("shutdown complete")
	return nil
}
