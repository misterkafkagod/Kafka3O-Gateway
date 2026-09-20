package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/telemetry"
)

// Compile-time assertions (TECH-SPEC §2.2 interface rule): the decorators
// satisfy the port interfaces they wrap.
var (
	_ kafka.Admin    = (*telemetry.TracedAdmin)(nil)
	_ kafka.Consumer = (*telemetry.TracedConsumer)(nil)
	_ kafka.Producer = (*telemetry.TracedProducer)(nil)
)

// TestTelemetry_DecoratorsSatisfyPortInterfaces exercises the assignments
// the package-level var block above only asserts at compile time: each
// constructor must build a value assignable to its port interface and
// usable through it.
func TestTelemetry_DecoratorsSatisfyPortInterfaces(t *testing.T) {
	t.Parallel()
	f := fake.New()

	var admin kafka.Admin = telemetry.NewTracedAdmin(f)
	var consumer kafka.Consumer = telemetry.NewTracedConsumer(f)
	var producer kafka.Producer = telemetry.NewTracedProducer(f)
	_, _ = consumer, producer

	if _, err := admin.DescribeCluster(context.Background()); err != nil {
		t.Fatalf("DescribeCluster() through the traced Admin port: %v", err)
	}
}

// Not t.Parallel(): NewProviders registers OTel's global tracer provider
// (TECH-SPEC §1.1), so tests that call it must run serially with one
// another to avoid cross-test contamination of that shared global state.
func TestTelemetry_ChildSpanPerKafkaCall(t *testing.T) {
	ctx := context.Background()
	exp := tracetest.NewInMemoryExporter()

	providers, err := telemetry.NewProviders(ctx, telemetry.Config{ServiceName: "test"}, telemetry.WithSpanExporter(exp))
	if err != nil {
		t.Fatalf("NewProviders() error: %v", err)
	}

	f := fake.New()
	f.SeedBroker(1, "broker1", 9092, "")
	traced := telemetry.NewTracedAdmin(f)

	// Success: one child span under the request's server span.
	reqCtx, serverSpan := providers.TracerProvider.Tracer("test").Start(ctx, "server")
	if _, err := traced.DescribeCluster(reqCtx); err != nil {
		t.Fatalf("DescribeCluster() error: %v", err)
	}
	serverSpan.End()

	// Failure: the same call, error status recorded on the span.
	f.FailNext("DescribeCluster", kafka.KindUnavailable)
	failCtx, failServerSpan := providers.TracerProvider.Tracer("test").Start(ctx, "server-fail")
	if _, err := traced.DescribeCluster(failCtx); err == nil {
		t.Fatal("DescribeCluster() with FailNext armed: want an error")
	}
	failServerSpan.End()

	if err := providers.TracerProvider.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush() error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 4 {
		t.Fatalf("spans recorded = %d, want 4 (2 server + 2 child)", len(spans))
	}

	foundOK, foundFail := false, false
	for _, s := range spans {
		if s.Name != "Admin.DescribeCluster" {
			continue
		}
		switch s.Parent.SpanID() {
		case serverSpan.SpanContext().SpanID():
			foundOK = true
			if s.Status.Code == codes.Error {
				t.Errorf("successful call's child span has error status: %+v", s.Status)
			}
		case failServerSpan.SpanContext().SpanID():
			foundFail = true
			if s.Status.Code != codes.Error {
				t.Errorf("failed call's child span has status %+v, want codes.Error", s.Status)
			}
		default:
			t.Errorf("child span %+v has an unexpected parent", s.Name)
		}
	}

	if !foundOK {
		t.Error("no successful Admin.DescribeCluster child span found under the server span")
	}
	if !foundFail {
		t.Error("no failed Admin.DescribeCluster child span found under the fail server span")
	}
}
