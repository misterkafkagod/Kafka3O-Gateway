package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func runServe(ctx context.Context, getenv func(string) string, listen func(string, string) (net.Listener, error), logger *slog.Logger) (result error) {
	config, err := loadServerConfig(getenv)
	if err != nil {
		return err
	}
	tlsConfig, err := loadServerTLS(config)
	if err != nil {
		return err
	}
	db, err := storage.OpenDatabase(ctx, config.databasePath)
	if err != nil {
		return fmt.Errorf("open Console store: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close Console store: %w", err))
		}
	}()
	if err := db.VerifyInitialAdministrator(ctx); err != nil {
		return fmt.Errorf("verify Console bootstrap: %w", err)
	}
	handler, err := newSessionHandler(db, config.origin, logger)
	if err != nil {
		return fmt.Errorf("initialize Console sessions: %w", err)
	}
	listener, err := listen("tcp", config.listen)
	if err != nil {
		return fmt.Errorf("listen for Console sessions: %w", err)
	}
	server := &http.Server{
		Handler: handler, TLSConfig: tlsConfig, ErrorLog: log.New(serverErrorLog{logger: logger}, "", 0),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	cleanupContext, stopCleanup := context.WithCancel(ctx)
	cleanupTicker := time.NewTicker(time.Minute)
	browserCleanupTicker := time.NewTicker(time.Minute)
	cleanupDone := make(chan struct{})
	browserCleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		maintainCognitoCredentials(cleanupContext, db, rand.Reader, time.Now, cleanupTicker.C, logger)
	}()
	go func() {
		defer close(browserCleanupDone)
		maintainBrowserBindings(cleanupContext, db, rand.Reader, time.Now, browserCleanupTicker.C, logger)
	}()
	defer func() {
		stopCleanup()
		cleanupTicker.Stop()
		browserCleanupTicker.Stop()
		<-cleanupDone
		<-browserCleanupDone
	}()
	logger.Info("Console session API listening", "address", listener.Addr().String())
	return serveConsole(ctx, server, listener)
}
