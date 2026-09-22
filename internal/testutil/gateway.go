// Package testutil is the gateway's one shared test scaffolding package
// (TECH-SPEC §4.9, §5.1): NewTestGateway wires a fake Kafka port behind a
// running httptest.Server so component tests across internal/api (and
// later phases) never touch a real cluster. It is a test double itself —
// depguard confines it to _test.go importers (TECH-SPEC P3, §5.3).
package testutil

import (
	"crypto/sha256"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/api"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	apitopic "github.com/misterkafkagod/kafka3o/internal/api/topic"
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// DefaultOperatorKey is the presented (unhashed) API key NewTestGateway
// grants operator tier by default.
const DefaultOperatorKey = "test-operator-secret"

// Gateway is a running gateway under test, backed by a fake Kafka port.
type Gateway struct {
	Server *httptest.Server
	Fake   *fake.Fake
	// Audit records every event M5-M7 report, for tests asserting on the
	// ATTEMPT/RESULT trail (FUNC-SPEC §8.5).
	Audit *audittest.RecordingSink
}

// Option customises the gateway NewTestGateway builds: either the fake's
// own construction-time options (WithClock, applied before fake.New) or a
// hook applied afterward, once both the Deps and the constructed *fake.Fake
// exist (WithAuthDisabled, WithUnreachable, WithCORSOrigins, WithKeys).
type Option func(*settings)

type settings struct {
	fakeOpts []fake.Option
	deps     api.Deps
	hooks    []func(*api.Deps, *fake.Fake)
}

// WithAuthDisabled turns off API-key enforcement (FUNC-SPEC D3).
func WithAuthDisabled() Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(deps *api.Deps, _ *fake.Fake) { deps.AuthEnabled = false })
	}
}

// WithUnreachable marks the fake Kafka port unreachable (FUNC-SPEC O6).
func WithUnreachable() Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(_ *api.Deps, f *fake.Fake) { f.Unreachable(true) })
	}
}

// WithCORSOrigins sets the allowed browser origins (FUNC-SPEC X5).
func WithCORSOrigins(origins ...string) Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(deps *api.Deps, _ *fake.Fake) { deps.CORSOrigins = origins })
	}
}

// WithKeys replaces the default operator key with keys (TECH-SPEC B4).
func WithKeys(keys ...middleware.Key) Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(deps *api.Deps, _ *fake.Fake) { deps.Keys = keys })
	}
}

// WithMaxBulkBodyBytes overrides M6's configured body-size ceiling
// (FUNC-SPEC §8.8), for tests exercising 413 without sending a
// multi-megabyte body.
func WithMaxBulkBodyBytes(n int64) Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(deps *api.Deps, _ *fake.Fake) { deps.MessageBounds.MaxBulkBodyBytes = n })
	}
}

// WithAuditUnhealthy makes /health/ready's audit sub-object report
// unhealthy, without affecting readiness itself (TECH-SPEC §6.1 B5).
func WithAuditUnhealthy() Option {
	return func(s *settings) {
		s.hooks = append(s.hooks, func(deps *api.Deps, _ *fake.Fake) {
			deps.AuditStatus = func() (string, bool) { return "kafka", false }
		})
	}
}

// defaultSettings is NewTestGateway's starting point before opts run: auth
// enabled, one operator key (DefaultOperatorKey), the real-time clock, and
// the same page-size default/ceiling as configs/config.example.yaml.
func defaultSettings() settings {
	return settings{
		deps: api.Deps{
			AuditStatus: func() (string, bool) { return "stdout", true },
			Keys: []middleware.Key{
				{ID: "test-operator", Tier: core.TierOperator, SHA256: sha256.Sum256([]byte(DefaultOperatorKey))},
			},
			AuthEnabled: true,
			PageBounds:  apitopic.PageBounds{Default: 50, Ceiling: 500},
			MessageBounds: message.Bounds{
				Limit:            message.Range{Default: 100, Ceiling: 1000},
				MaxScan:          message.Range{Default: 10000, Ceiling: 100000},
				MaxMatches:       message.Range{Default: 100, Ceiling: 1000},
				MaxBytes:         message.RangeBytes{Default: 10 * 1024 * 1024, Ceiling: 100 * 1024 * 1024},
				MaxTime:          message.RangeDuration{Default: 10 * time.Second, Ceiling: 60 * time.Second},
				RegexTimeout:     100 * time.Millisecond,
				MaxBulkBodyBytes: 10 * 1024 * 1024,
			},
		},
	}
}

// NewTestGateway builds a fake-backed gateway behind an httptest.Server.
// The server is closed automatically when t completes.
func NewTestGateway(t *testing.T, opts ...Option) *Gateway {
	t.Helper()

	s := defaultSettings()
	for _, opt := range opts {
		opt(&s)
	}

	f := fake.New(s.fakeOpts...)
	rec := audittest.New()
	s.deps.Cluster = f
	s.deps.Admin = f
	s.deps.Producer = f
	s.deps.Auditor = audit.NewAuditor(rec)
	s.deps.NewConsumer = func() (kafka.Consumer, error) { return f, nil }
	for _, hook := range s.hooks {
		hook(&s.deps, f)
	}

	srv := httptest.NewServer(api.New(s.deps))
	t.Cleanup(srv.Close)
	return &Gateway{Server: srv, Fake: f, Audit: rec}
}
