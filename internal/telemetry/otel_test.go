package telemetry_test

import (
	"context"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/misterkafkagod/kafka3o/internal/telemetry"
)

// recordingExporter is a minimal SpanExporter that, unlike
// go.opentelemetry.io/otel/sdk/trace/tracetest.InMemoryExporter, keeps its
// recorded spans after Shutdown — needed to prove Shutdown actually
// flushed pending spans into the exporter before closing it, rather than
// silently dropping them.
type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (r *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

func (r *recordingExporter) Shutdown(context.Context) error { return nil }

func (r *recordingExporter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.spans)
}

// Not t.Parallel(): NewProviders registers OTel's global tracer provider
// (TECH-SPEC §1.1), so tests that call it must run serially with one
// another to avoid cross-test contamination of that shared global state.
func TestTelemetry_ShutdownFlushesExporters(t *testing.T) {
	ctx := context.Background()
	exp := &recordingExporter{}

	providers, err := telemetry.NewProviders(ctx, telemetry.Config{ServiceName: "test"}, telemetry.WithSpanExporter(exp))
	if err != nil {
		t.Fatalf("NewProviders() error: %v", err)
	}

	_, span := providers.TracerProvider.Tracer("test").Start(ctx, "unflushed")
	span.End()

	// The batch processor holds the span until flushed — Shutdown must
	// still export it, not drop it (TECH-SPEC §5.4 shutdown order).
	if got := exp.count(); got != 0 {
		t.Fatalf("spans exported before Shutdown = %d, want 0 (batched, not yet flushed)", got)
	}

	if err := providers.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}

	if got := exp.count(); got != 1 {
		t.Fatalf("spans exported after Shutdown = %d, want 1", got)
	}
}
