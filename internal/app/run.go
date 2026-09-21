// Package app is the gateway's composition root (TECH-SPEC §2.0 P1–P3,
// §2.1, D2, Y5): the only package that imports both internal/kafka/franz
// (the real adapter) and every other internal package, wiring a validated
// config.Config into a running server. cmd/gateway is a thin entrypoint
// that only calls Run (TECH-SPEC §5.3).
package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/misterkafkagod/kafka3o/internal/api"
	"github.com/misterkafkagod/kafka3o/internal/api/health"
	apitopic "github.com/misterkafkagod/kafka3o/internal/api/topic"
	"github.com/misterkafkagod/kafka3o/internal/config"
	"github.com/misterkafkagod/kafka3o/internal/kafka/franz"
	"github.com/misterkafkagod/kafka3o/internal/telemetry"
)

// Run wires cfg into a running gateway and blocks until ctx is cancelled
// (SIGTERM), then drains and shuts everything down in order (TECH-SPEC
// §2.1 P1–P3, §5.4): config → telemetry → franz client → api → http.Server.
func Run(ctx context.Context, cfg config.Config) error {
	logger := telemetry.NewLogger(os.Stdout, cfg.Telemetry.ServiceName, telemetry.WithLevel(parseLevel(cfg.Telemetry.LogLevel)))
	slog.SetDefault(logger)

	providers, err := telemetry.NewProviders(ctx, telemetry.Config{
		ServiceName:  cfg.Telemetry.ServiceName,
		OTLPEndpoint: cfg.Telemetry.OTLP.Endpoint,
		OTLPProtocol: cfg.Telemetry.OTLP.Protocol,
	})
	if err != nil {
		return fmt.Errorf("app: telemetry: %w", err)
	}

	client, err := franz.New(franz.Config{
		Bootstrap:      cfg.Kafka.Bootstrap,
		RequestTimeout: cfg.Kafka.RequestTimeout,
		TLS: franz.TLSConfig{
			Enabled:  cfg.Kafka.TLS.Enabled,
			CAFile:   cfg.Kafka.TLS.CAFile,
			CertFile: cfg.Kafka.TLS.CertFile,
			KeyFile:  cfg.Kafka.TLS.KeyFile,
		},
		SASL: franz.SASLConfig{
			Mechanism: cfg.Kafka.SASL.Mechanism,
			Username:  cfg.Kafka.SASL.Username,
			Password:  cfg.Kafka.SASL.Password,
		},
	})
	if err != nil {
		return fmt.Errorf("app: kafka client: %w", err)
	}

	handler, err := newHandler(cfg, client)
	if err != nil {
		return err
	}

	server, err := newServer(cfg.HTTP, handler)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", cfg.HTTP.Addr)
	if err != nil {
		return fmt.Errorf("app: listen on %s: %w", cfg.HTTP.Addr, err)
	}
	logger.Info("listening on " + cfg.HTTP.Addr)

	// The shutdown grace period is the same "bounded request + headroom"
	// value already derived onto WriteTimeout (TECH-SPEC §5.4, C6): no
	// in-flight request can legitimately still be running past it.
	return runServer(ctx, listener, server, cfg.HTTP.WriteTimeout, providers, client, logger)
}

// newServer builds the *http.Server for h (TECH-SPEC §6.3 C6): timeouts
// copied verbatim from the already-validated config, and native TLS when a
// certificate is configured.
func newServer(h config.HTTP, handler http.Handler) (*http.Server, error) {
	server := &http.Server{
		Addr:              h.Addr,
		Handler:           handler,
		ReadHeaderTimeout: h.ReadHeaderTimeout,
		IdleTimeout:       h.IdleTimeout,
		WriteTimeout:      h.WriteTimeout,
	}
	if h.TLS.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(h.TLS.CertFile, h.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("app: HTTP TLS certificate: %w", err)
		}
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	}
	return server, nil
}

// newHandler builds the gateway's HTTP handler from cfg and an already-built
// Kafka client (TECH-SPEC §2.1).
func newHandler(cfg config.Config, client *franz.Client) (http.Handler, error) {
	keys, err := mapKeys(cfg.Auth.Keys)
	if err != nil {
		return nil, err
	}
	trustedProxies, err := parseCIDRs(cfg.HTTP.TrustedProxies)
	if err != nil {
		return nil, err
	}

	admin := telemetry.NewTracedAdmin(client)
	return api.New(api.Deps{
		Cluster:        admin,
		Admin:          admin,
		PageBounds:     apitopic.PageBounds{Default: cfg.Bounds.Page.Size.Default, Ceiling: cfg.Bounds.Page.Size.Ceiling},
		AuditStatus:    auditStatus(cfg.Audit),
		Keys:           keys,
		AuthEnabled:    cfg.Auth.Enabled,
		CORSOrigins:    cfg.HTTP.CORS.Origins,
		TrustedProxies: trustedProxies,
		DocsEnabled:    cfg.HTTP.Docs.Enabled,
	}), nil
}

// auditStatus reports the configured sink as healthy: the real audit sink
// (Phase 5, F5) does not exist yet, so there is nothing it could currently
// report as unhealthy (TECH-SPEC §6.1 B5). Phase 5 replaces this with a
// function backed by the sink's actual writability check.
func auditStatus(a config.Audit) health.AuditStatus {
	return func() (string, bool) { return a.Sink, true }
}
