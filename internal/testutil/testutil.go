// Package testutil is the gateway's one shared test scaffolding package
// (TECH-SPEC §4.5): NewTestGateway wires a fake Kafka port behind a running
// httptest.Server so component tests across internal/api (and later
// phases) never touch a real cluster. It is a test double itself —
// depguard confines it to _test.go importers (TECH-SPEC P3, §5.3).
package testutil

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// DefaultOperatorKey is the presented (unhashed) API key NewTestGateway
// grants operator tier by default.
const DefaultOperatorKey = "test-operator-secret"

// Gateway is a running gateway under test, backed by a fake Kafka port.
type Gateway struct {
	Server *httptest.Server
	Fake   *fake.Fake
}

// Option customises the Deps or fake NewTestGateway builds before the
// server starts.
type Option func(*api.Deps, *fake.Fake)

// WithAuthDisabled turns off API-key enforcement (FUNC-SPEC D3).
func WithAuthDisabled() Option {
	return func(deps *api.Deps, _ *fake.Fake) { deps.AuthEnabled = false }
}

// WithUnreachable marks the fake Kafka port unreachable (FUNC-SPEC O6).
func WithUnreachable() Option {
	return func(_ *api.Deps, f *fake.Fake) { f.Unreachable(true) }
}

// WithCORSOrigins sets the allowed browser origins (FUNC-SPEC X5).
func WithCORSOrigins(origins ...string) Option {
	return func(deps *api.Deps, _ *fake.Fake) { deps.CORSOrigins = origins }
}

// NewTestGateway builds a fake-backed gateway behind an httptest.Server.
// The server is closed automatically when t completes.
func NewTestGateway(t *testing.T, opts ...Option) *Gateway {
	t.Helper()

	f := fake.New()
	deps := api.Deps{
		Cluster:     f,
		AuditStatus: func() (string, bool) { return "stdout", true },
		Keys: []middleware.Key{
			{ID: "test-operator", Tier: core.TierOperator, SHA256: sha256.Sum256([]byte(DefaultOperatorKey))},
		},
		AuthEnabled: true,
	}
	for _, opt := range opts {
		opt(&deps, f)
	}

	srv := httptest.NewServer(api.New(deps))
	t.Cleanup(srv.Close)
	return &Gateway{Server: srv, Fake: f}
}

// Get issues a GET request against the running gateway, presenting
// DefaultOperatorKey (FUNC-SPEC B3: no route is exempt from authentication,
// including /health/* and /openapi.json).
func (gw *Gateway) Get(t *testing.T, path string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, gw.Server.URL+path, nil)
	if err != nil {
		t.Fatalf("http.NewRequest(%q) error: %v", path, err)
	}
	req.Header.Set(middleware.HeaderAPIKey, DefaultOperatorKey)

	resp, err := gw.Server.Client().Do(req)
	if err != nil {
		t.Fatalf("GET %s error: %v", path, err)
	}
	return resp
}
