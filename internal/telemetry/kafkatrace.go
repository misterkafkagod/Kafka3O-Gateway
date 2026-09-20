package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// tracerName identifies every span this file creates (TECH-SPEC §4.5
// Telemetry, Step 11 G3).
const tracerName = "github.com/misterkafkagod/kafka3o/internal/telemetry"

// TracedAdmin wraps a kafka.Admin, recording one child span per call —
// named "Admin.<Method>", with the call's error recorded as the span's
// status (TECH-SPEC §4.5 Telemetry, Step 11 G3, spec note N3).
type TracedAdmin struct {
	kafka.Admin
}

// NewTracedAdmin wraps next with tracing.
func NewTracedAdmin(next kafka.Admin) *TracedAdmin {
	return &TracedAdmin{Admin: next}
}

// DescribeCluster wraps kafka.Admin.DescribeCluster in a child span (C1).
func (t *TracedAdmin) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	ctx, span := startSpan(ctx, "Admin.DescribeCluster")
	defer span.End()

	info, err := t.Admin.DescribeCluster(ctx)
	recordResult(span, err)
	return info, err
}

// TracedConsumer wraps a kafka.Consumer. Consumer has no methods yet
// (Phase 3, Task 3.1 adds them); each one gains its own traced override
// here, following the same one-child-span-per-call pattern as TracedAdmin.
type TracedConsumer struct {
	kafka.Consumer
}

// NewTracedConsumer wraps next with tracing.
func NewTracedConsumer(next kafka.Consumer) *TracedConsumer {
	return &TracedConsumer{Consumer: next}
}

// TracedProducer wraps a kafka.Producer. Producer has no methods yet
// (Phase 5, Task 5.1 adds them); each one gains its own traced override
// here, following the same one-child-span-per-call pattern as TracedAdmin.
type TracedProducer struct {
	kafka.Producer
}

// NewTracedProducer wraps next with tracing.
func NewTracedProducer(next kafka.Producer) *TracedProducer {
	return &TracedProducer{Producer: next}
}

// startSpan starts a child span named name under whatever span is active
// in ctx (the request's server span, when called from a request path).
func startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name)
}

// recordResult records err on span as its status, if the call failed.
func recordResult(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
