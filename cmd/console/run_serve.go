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

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
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
	cognitoSettings, cognitoConfigured, err := loadCognitoConfig(getenv, config.origin)
	if err != nil {
		return err
	}
	clusters, clustersConfigured, err := loadClusterRegistry(getenv)
	if err != nil {
		return err
	}
	policy, policyConfigured, err := loadPolicyConfig(getenv)
	if err != nil {
		return err
	}
	forwarding, forwardingConfigured, err := loadForwardingConfig(getenv)
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
	if clustersConfigured {
		operationID, err := identity.NewOperationID(rand.Reader)
		if err != nil {
			return err
		}
		if err := db.SyncRegisteredClusters(ctx, clusters, operationID, time.Now()); err != nil {
			return fmt.Errorf("sync registered clusters: %w", err)
		}
	}
	handler, err := newSessionHandler(db, config.origin, logger)
	if err != nil {
		return fmt.Errorf("initialize Console sessions: %w", err)
	}
	if forwardingConfigured {
		if err := enableGatewayRequests(handler, db, forwarding); err != nil {
			return fmt.Errorf("initialize Console Gateway forwarding: %w", err)
		}
	}
	if cognitoConfigured {
		if err := handler.EnableCognito(rand.Reader); err != nil {
			return fmt.Errorf("initialize Console Cognito sign-in: %w", err)
		}
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
	discoveryDone := make(chan struct{})
	if cognitoConfigured {
		go func() {
			defer close(discoveryDone)
			discoverCognito(cleanupContext, func(ctx context.Context) (consoleapi.CognitoServices, error) {
				return newCognitoServices(ctx, db, cognitoSettings, rand.Reader, time.Now)
			}, handler.PublishCognito, func(attempt int) <-chan time.Time {
				return time.After(cognitoDiscoveryDelay(attempt))
			}, logger)
		}()
	} else {
		close(discoveryDone)
	}
	defer func() {
		stopCleanup()
		cleanupTicker.Stop()
		browserCleanupTicker.Stop()
		<-cleanupDone
		<-browserCleanupDone
		<-discoveryDone
	}()
	policyDone := make(chan error, 1)
	serveContext, stopServing := context.WithCancel(ctx)
	defer stopServing()
	if policyConfigured {
		policyHandler, err := newPolicyHandler(db, logger)
		if err != nil {
			listener.Close()
			return fmt.Errorf("initialize Console policy API: %w", err)
		}
		policyListener, err := listen("tcp", policy.listen)
		if err != nil {
			listener.Close()
			return fmt.Errorf("listen for Console policy API: %w", err)
		}
		policyServer := &http.Server{
			Handler: policyHandler, TLSConfig: policy.tls, ErrorLog: log.New(serverErrorLog{logger: logger}, "", 0),
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
			IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 << 10,
		}
		logger.Info("Console policy API listening", "address", policyListener.Addr().String())
		go func() {
			err := serveConsole(serveContext, policyServer, policyListener)
			stopServing() // a failed policy listener stops the session API as well
			policyDone <- err
		}()
	} else {
		policyDone <- nil
	}
	logger.Info("Console session API listening", "address", listener.Addr().String())
	serveErr := serveConsole(serveContext, server, listener)
	stopServing()
	return errors.Join(serveErr, <-policyDone)
}
