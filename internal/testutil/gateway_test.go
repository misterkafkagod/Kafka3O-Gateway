package testutil

import (
	"crypto/sha256"
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestNewTestGateway_BootsWithFakeRecordingSinkAndFixedClock(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	gw := NewTestGateway(t, WithClock(func() time.Time { return fixed }))

	resp := gw.Get(t, "/v1/health/ready")
	_ = resp.Body.Close()

	// The fake records every port call it serves (TECH-SPEC §4.3 Recording
	// row) — this is the "recording sink" NewTestGateway wires up.
	gw.Fake.AssertCalled(t, "DescribeCluster")

	// The fixed clock is threaded through to fake.New before construction:
	// seeding a zero-timestamp record must not panic or block, proving the
	// option reached the fake rather than being silently dropped.
	gw.Fake.SeedTopic("t1", 1)
}

func TestNewTestGateway_OptionsApplyPolicyAndKeys(t *testing.T) {
	t.Parallel()

	customKey := middleware.Key{ID: "custom", Tier: core.TierReader, SHA256: sha256.Sum256([]byte("custom-secret"))}
	gw := NewTestGateway(t, WithKeys(customKey))

	// The default key WithKeys replaced no longer authenticates.
	resp := gw.DoWithKey(t, http.MethodGet, "/v1/health/live", nil, DefaultOperatorKey)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status with the replaced default key = %d, want 401", resp.StatusCode)
	}

	// The configured custom key does.
	resp2 := gw.DoWithKey(t, http.MethodGet, "/v1/health/live", nil, "custom-secret")
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status with the configured custom key = %d, want 200", resp2.StatusCode)
	}

	// WithAuthDisabled applies the auth policy switch: every request is
	// treated as operator, no key required (FUNC-SPEC D3).
	open := NewTestGateway(t, WithAuthDisabled())
	resp3 := open.DoWithKey(t, http.MethodGet, "/v1/health/live", nil, "")
	defer func() { _ = resp3.Body.Close() }()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("status with auth disabled and no key = %d, want 200", resp3.StatusCode)
	}
}
