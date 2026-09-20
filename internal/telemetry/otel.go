// Package telemetry wires the gateway's OpenTelemetry tracer and meter
// providers, OTLP export, structured logging, and Kafka-port tracing
// decorators (TECH-SPEC §1.0, §1.1, §4.5, §5.1). It may import internal/kafka
// (the port, for the tracing decorators) but never internal/config
// (TECH-SPEC §5.3): it takes only plain values, so the composition root
// (Task 1.10) is the only place that maps config.Telemetry onto Config.
package telemetry

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Config are the gateway's telemetry start-up settings (TECH-SPEC §1.1). An
// empty OTLPEndpoint disables OTLP export (matching internal/config.OTLP).
type Config struct {
	ServiceName  string
	OTLPEndpoint string
	OTLPProtocol string // "grpc" or "http"
}

// Providers holds the tracer and meter providers NewProviders builds. It
// installs them as OTel's global providers (otel.SetTracerProvider,
// otel.SetMeterProvider) so franz-go's kotel plugin and every
// otel.Tracer/otel.Meter call in this process pick them up without further
// wiring (TECH-SPEC §1.1).
type Providers struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
}

// Option customises NewProviders.
type Option func(*settings)

type settings struct {
	spanExporter sdktrace.SpanExporter
}

// WithSpanExporter overrides the OTLP trace exporter (or its absence, when
// Config.OTLPEndpoint is empty) with exp. Tests use this to inject an
// in-memory exporter such as go.opentelemetry.io/otel/sdk/trace/tracetest
// (TECH-SPEC §4.5 Telemetry).
func WithSpanExporter(exp sdktrace.SpanExporter) Option {
	return func(s *settings) { s.spanExporter = exp }
}

// NewProviders builds the tracer and meter providers for cfg, wiring OTLP
// export when cfg.OTLPEndpoint is set (TECH-SPEC §1.1).
func NewProviders(ctx context.Context, cfg Config, opts ...Option) (*Providers, error) {
	var s settings
	for _, opt := range opts {
		opt(&s)
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)))
	if err != nil {
		return nil, fmt.Errorf("telemetry: build resource: %w", err)
	}

	tp, err := newTracerProvider(ctx, cfg, res, s.spanExporter)
	if err != nil {
		return nil, err
	}
	mp, err := newMeterProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)

	return &Providers{TracerProvider: tp, MeterProvider: mp}, nil
}

func newTracerProvider(ctx context.Context, cfg Config, res *resource.Resource, testExporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	switch {
	case testExporter != nil:
		// Same processor as production (WithBatcher): a test asserting
		// Shutdown/ForceFlush actually flushes must exercise the real
		// batching path, not a synchronous shortcut.
		opts = append(opts, sdktrace.WithBatcher(testExporter))
	case cfg.OTLPEndpoint != "":
		exp, err := newSpanExporter(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("telemetry: build span exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	return sdktrace.NewTracerProvider(opts...), nil
}

func newMeterProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}

	if cfg.OTLPEndpoint != "" {
		exp, err := newMetricExporter(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("telemetry: build metric exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}

	return sdkmetric.NewMeterProvider(opts...), nil
}

func newSpanExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	if cfg.OTLPProtocol == "http" {
		return otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint))
	}
	return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint))
}

func newMetricExporter(ctx context.Context, cfg Config) (sdkmetric.Exporter, error) {
	if cfg.OTLPProtocol == "http" {
		return otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpoint(cfg.OTLPEndpoint))
	}
	return otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint))
}

// Shutdown flushes and closes both providers (TECH-SPEC §5.4 shutdown
// order): the tracer provider's Shutdown drains any batched spans before
// the meter provider's Shutdown flushes pending metrics.
func (p *Providers) Shutdown(ctx context.Context) error {
	return errors.Join(p.TracerProvider.Shutdown(ctx), p.MeterProvider.Shutdown(ctx))
}
