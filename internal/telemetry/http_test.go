package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/misterkafkagod/kafka3o/internal/telemetry"
)

// TestTelemetry_OneServerSpanPerRequestWithRequestIDAttribute is not
// t.Parallel(): NewProviders registers OTel's global tracer provider
// (TECH-SPEC §1.1, matching kotel's own global-provider convention), so
// tests that call it must run serially with one another to avoid
// cross-test contamination of that shared global state.
func TestTelemetry_OneServerSpanPerRequestWithRequestIDAttribute(t *testing.T) {
	ctx := context.Background()
	exp := tracetest.NewInMemoryExporter()

	providers, err := telemetry.NewProviders(ctx, telemetry.Config{ServiceName: "test"}, telemetry.WithSpanExporter(exp))
	if err != nil {
		t.Fatalf("NewProviders() error: %v", err)
	}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := telemetry.HTTPMiddleware("gateway", func(r *http.Request) string {
		return r.Header.Get("X-Request-Id")
	})(inner)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/health/live", nil)
	req.Header.Set("X-Request-Id", "req-789")
	handler.ServeHTTP(rec, req)

	if err := providers.TracerProvider.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush() error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans recorded = %d, want exactly 1 server span per request", len(spans))
	}

	span := spans[0]
	var gotRequestID any
	for _, attr := range span.Attributes {
		if string(attr.Key) == "requestId" {
			gotRequestID = attr.Value.AsString()
		}
	}
	if gotRequestID != "req-789" {
		t.Errorf("span requestId attribute = %v, want req-789", gotRequestID)
	}
}
