package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

func serveConsole(ctx context.Context, server *http.Server, listener net.Listener) error {
	done := make(chan error, 1)
	go func() {
		done <- server.ServeTLS(listener, "", "")
	}()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("serve Console sessions: %w", err)
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdown)
		var closeErr error
		if shutdownErr != nil {
			closeErr = server.Close()
		}
		serveErr := <-done
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		if shutdownErr != nil {
			shutdownErr = fmt.Errorf("shut down Console sessions: %w", shutdownErr)
		}
		if serveErr != nil {
			serveErr = fmt.Errorf("serve Console sessions: %w", serveErr)
		}
		return errors.Join(shutdownErr, closeErr, serveErr)
	}
}
