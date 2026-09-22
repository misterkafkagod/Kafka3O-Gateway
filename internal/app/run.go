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
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/config"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/franz"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
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

	client, err := franz.New(franzConfig(cfg))
	if err != nil {
		return fmt.Errorf("app: kafka client: %w", err)
	}
	closers := []kafkaCloser{client}

	auditor, auditHealthy, auditClient, err := newAuditor(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("app: audit: %w", err)
	}
	if auditClient != nil {
		closers = append(closers, auditClient)
	}

	handler, err := newHandler(cfg, client, auditor, auditHealthy)
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
	return runServer(ctx, listener, server, cfg.HTTP.WriteTimeout, providers, closers, logger)
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

// newHandler builds the gateway's HTTP handler from cfg, an already-built
// Kafka client (TECH-SPEC §2.1), and the already-built Auditor and its
// start-up health.
func newHandler(cfg config.Config, client *franz.Client, auditor *audit.Auditor, auditHealthy bool) (http.Handler, error) {
	keys, err := mapKeys(cfg.Auth.Keys)
	if err != nil {
		return nil, err
	}
	trustedProxies, err := parseCIDRs(cfg.HTTP.TrustedProxies)
	if err != nil {
		return nil, err
	}

	admin := telemetry.NewTracedAdmin(client)
	producer := telemetry.NewTracedProducer(client)
	return api.New(api.Deps{
		Cluster:     admin,
		Admin:       admin,
		Producer:    producer,
		Auditor:     auditor,
		Policy:      mapPolicy(cfg.Auth.Enabled, cfg.Policy),
		PageBounds:  apitopic.PageBounds{Default: cfg.Bounds.Page.Size.Default, Ceiling: cfg.Bounds.Page.Size.Ceiling},
		NewConsumer: newConsumerFactory(cfg),
		MessageBounds: message.Bounds{
			Limit:            message.Range{Default: cfg.Bounds.Read.Limit.Default, Ceiling: cfg.Bounds.Read.Limit.Ceiling},
			MaxScan:          message.Range{Default: cfg.Bounds.Scan.MaxScan.Default, Ceiling: cfg.Bounds.Scan.MaxScan.Ceiling},
			MaxMatches:       message.Range{Default: cfg.Bounds.Scan.MaxMatches.Default, Ceiling: cfg.Bounds.Scan.MaxMatches.Ceiling},
			MaxBytes:         message.RangeBytes{Default: int64(cfg.Bounds.Scan.MaxBytes.Default), Ceiling: int64(cfg.Bounds.Scan.MaxBytes.Ceiling)},
			MaxTime:          message.RangeDuration{Default: cfg.Bounds.Scan.MaxTime.Default, Ceiling: cfg.Bounds.Scan.MaxTime.Ceiling},
			RegexTimeout:     cfg.Bounds.Scan.RegexTimeout,
			MaxBulkBodyBytes: int64(cfg.Bounds.BulkProduceBody),
		},
		AuditStatus:    auditStatus(cfg.Audit, auditHealthy),
		Keys:           keys,
		AuthEnabled:    cfg.Auth.Enabled,
		CORSOrigins:    cfg.HTTP.CORS.Origins,
		TrustedProxies: trustedProxies,
		DocsEnabled:    cfg.HTTP.Docs.Enabled,
	}), nil
}

// franzConfig maps cfg's Kafka connection settings onto franz.Config, shared
// by the long-lived admin client (Run) and every dedicated per-scan Consumer
// (newConsumerFactory) so both reach the same cluster the same way.
func franzConfig(cfg config.Config) franz.Config {
	return franz.Config{
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
	}
}

// newConsumerFactory returns a message.ConsumerFactory building a fresh,
// dedicated kgo.Client per scan (TECH-SPEC §2.3), at cfg's configured
// isolation level (TECH-SPEC C4).
func newConsumerFactory(cfg config.Config) message.ConsumerFactory {
	return func() (kafka.Consumer, error) {
		return franz.NewConsumer(franzConfig(cfg), cfg.Kafka.Consumer.IsolationLevel)
	}
}

// auditStatus reports the configured sink and its start-up health (TECH-SPEC
// §6.1 B5): computed once by newAuditor and held fixed for the process
// lifetime (FUNC-SPEC §9.6 — nothing here is per-request state).
func auditStatus(a config.Audit, healthy bool) health.AuditStatus {
	return func() (string, bool) { return a.Sink, healthy }
}

// newAuditor builds the gateway's Auditor (FUNC-SPEC §8.5): the stdout/slog
// sink always runs. When cfg.Audit.Sink is "kafka", it also builds a
// dedicated producer client for the Kafka sink (TECH-SPEC C5, separate from
// the data-plane producer) and wires that sink in regardless of the start-up
// check below — a topic that is really unwritable then fails closed on its
// own, on every real ATTEMPT (FUNC-SPEC §8.5 V2), exactly like any other
// sink failure. The returned bool and *franz.Client are that check's result
// and the dedicated client to close at shutdown (nil when sink is "stdout").
func newAuditor(ctx context.Context, cfg config.Config, logger *slog.Logger) (*audit.Auditor, bool, *franz.Client, error) {
	sinks := []audit.Sink{audit.NewSlogSink(logger)}
	if cfg.Audit.Sink != config.AuditSinkKafka {
		return audit.NewAuditor(sinks...), true, nil, nil
	}

	auditClient, err := franz.New(franzConfig(cfg))
	if err != nil {
		return nil, false, nil, fmt.Errorf("app: audit kafka client: %w", err)
	}
	auditProducer := telemetry.NewTracedProducer(auditClient)
	sinks = append(sinks, audit.NewKafkaSink(auditProducer, cfg.Audit.Topic))

	verifyCtx, cancel := context.WithTimeout(ctx, cfg.Kafka.RequestTimeout)
	defer cancel()
	healthy := auditHealthAfterVerify(verifyCtx, telemetry.NewTracedAdmin(auditClient), auditProducer, cfg.Audit.Topic, logger)

	return audit.NewAuditor(sinks...), healthy, auditClient, nil
}

// auditHealthAfterVerify runs verifyAuditTopic and reports whether it
// succeeded, logging an ERROR when it did not (TECH-SPEC §6.1 B5) — split
// from newAuditor so it is testable against a fake kafka.Admin/kafka.Producer
// without a real *franz.Client.
func auditHealthAfterVerify(ctx context.Context, admin kafka.Admin, producer kafka.Producer, topic string, logger *slog.Logger) bool {
	if err := verifyAuditTopic(ctx, admin, producer, topic); err != nil {
		logger.Error("audit topic verification failed", "topic", topic, "error", err)
		return false
	}
	return true
}

// verifyAuditTopic checks that topic exists and accepts a write (TECH-SPEC
// §6.1 B5): the audit topic is never auto-created, so either failure is an
// operator-configuration error to surface, not one the gateway works around.
func verifyAuditTopic(ctx context.Context, admin kafka.Admin, producer kafka.Producer, topic string) error {
	if _, err := admin.DescribeTopics(ctx, topic); err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	results, err := producer.Produce(ctx, topic, []kafka.ProduceRequest{{Value: []byte(`{"probe":"startup"}`)}})
	if err != nil {
		return fmt.Errorf("probe produce: %w", err)
	}
	if len(results) > 0 && results[0].Err != nil {
		return fmt.Errorf("probe produce: %w", results[0].Err)
	}
	return nil
}
