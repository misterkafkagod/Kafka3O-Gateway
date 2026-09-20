package telemetry

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware wraps next in one OTel HTTP server span per request
// (named operation), then attaches requestID(r)'s result to that span as a
// "requestId" attribute (TECH-SPEC §4.5 Telemetry). internal/telemetry may
// not import internal/api/errors (TECH-SPEC §5.3), so the caller — the
// composition root — supplies how to read the request id already stored on
// r's context by the request-id middleware (Task 1.8.2).
func HTTPMiddleware(operation string, requestID func(r *http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		annotated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if id := requestID(r); id != "" {
				trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("requestId", id))
			}
			next.ServeHTTP(w, r)
		})
		return otelhttp.NewHandler(annotated, operation)
	}
}
