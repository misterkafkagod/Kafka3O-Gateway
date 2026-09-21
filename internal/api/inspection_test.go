package api_test

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"testing"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

// readerKey is a reader-tier key for tests exercising C1, C2, C4, T1-T4:
// every one of them is an R command, reachable by either tier (FUNC-SPEC §9.1).
const readerSecret = "test-reader-secret"

func readerGateway(t *testing.T) *testutil.Gateway {
	t.Helper()
	key := middleware.Key{ID: "test-reader", Tier: core.TierReader, SHA256: sha256.Sum256([]byte(readerSecret))}
	return testutil.NewTestGateway(t, testutil.WithKeys(key))
}

func TestAPI_ErrorMapping_EveryKindThroughHTTP(t *testing.T) {
	t.Parallel()
	for _, kind := range kafka.Kinds() {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			gw := testutil.NewTestGateway(t)
			gw.Fake.FailNext("DescribeTopics", kind)

			resp := gw.Get(t, "/v1/topics/whatever")
			defer func() { _ = resp.Body.Close() }()

			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode response body: %v", err)
			}
			errBody, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("response has no error object: %v", body)
			}

			// FailNext injects a bare *kafka.Error with no broker code/name
			// (fake/fake.go's invoke), so the expected envelope carries none
			// either — this proves the Kind -> status/code mapping alone.
			want := apierrors.Map(&kafka.Error{Kind: kind}, "req-1")
			if resp.StatusCode != want.GetStatus() {
				t.Errorf("status = %d, want %d", resp.StatusCode, want.GetStatus())
			}
			if errBody["code"] != want.ErrorBody.Code {
				t.Errorf("code = %v, want %v", errBody["code"], want.ErrorBody.Code)
			}
			if errBody["requestId"] == "" || errBody["requestId"] == nil {
				t.Error("requestId missing from error envelope")
			}
		})
	}
}

func TestAPI_Inspection_ReaderKeyAllowedOnAllRoutes(t *testing.T) {
	t.Parallel()
	gw := readerGateway(t)
	gw.Fake.SeedBroker(1, "b1", 9092, "")
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Value: []byte("v")})
	gw.Fake.SeedPartitionMeta("t-demo", 0, 1, []int32{1}, []int32{1})

	routes := []string{
		"/v1/cluster",
		"/v1/cluster/brokers/1/config",
		"/v1/cluster/health",
		"/v1/topics",
		"/v1/topics/t-demo",
		"/v1/topics/t-demo/size",
		"/v1/topics/t-demo/count?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z",
	}
	for _, route := range routes {
		resp := gw.DoWithKey(t, http.MethodGet, route, nil, readerSecret)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s with reader key = %d, want 200", route, resp.StatusCode)
		}
	}
}

func TestAPI_T1_InvalidPattern400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Get(t, "/v1/topics?pattern=(")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "INVALID_REGEX" {
		t.Errorf("code = %v, want INVALID_REGEX", errBody["code"])
	}
}

func TestAPI_T1_PaginationEnvelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 3, kafka.Record{Value: []byte("v")})

	resp := gw.Get(t, "/v1/topics?pattern=^t-")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("items = %v, want 1 entry", items)
	}
	first, _ := items[0].(map[string]any)
	if first["name"] != "t-demo" {
		t.Errorf("items[0].name = %v, want t-demo", first["name"])
	}
	page, _ := body["page"].(map[string]any)
	if page["total"] != float64(1) {
		t.Errorf("page.total = %v, want 1", page["total"])
	}

	// A page past the end reports empty items with the same total
	// (TECH-SPEC §4.5 Pagination).
	resp2 := gw.Get(t, "/v1/topics?page=99")
	defer func() { _ = resp2.Body.Close() }()
	var body2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&body2)
	items2, _ := body2["items"].([]any)
	if len(items2) != 0 {
		t.Errorf("items past the end = %v, want empty", items2)
	}
	page2, _ := body2["page"].(map[string]any)
	if page2["total"] != float64(1) {
		t.Errorf("page.total past the end = %v, want 1", page2["total"])
	}
}

func TestAPI_T2_Missing404Envelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Get(t, "/v1/topics/does-not-exist")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "NOT_FOUND" {
		t.Errorf("code = %v, want NOT_FOUND", errBody["code"])
	}
	details, _ := errBody["details"].(map[string]any)
	if details["resource"] != "topic" {
		t.Errorf("details.resource = %v, want topic", details["resource"])
	}
}

func TestAPI_C2_SensitiveMasked(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedBroker(1, "b1", 9092, "")
	gw.Fake.SeedBrokerConfigs(1,
		kafka.ConfigEntry{Name: "log.retention.ms", Value: "604800000"},
		kafka.ConfigEntry{Name: "sasl.jaas.config", Value: "super-secret", IsSensitive: true},
	)

	resp := gw.Get(t, "/v1/cluster/brokers/1/config")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	configs, _ := body["configs"].([]any)
	var sawSensitive bool
	for _, raw := range configs {
		c, _ := raw.(map[string]any)
		if c["name"] != "sasl.jaas.config" {
			continue
		}
		sawSensitive = true
		if c["value"] != nil {
			t.Errorf("sensitive config value = %v, want null", c["value"])
		}
		if c["isSensitive"] != true {
			t.Errorf("isSensitive = %v, want true", c["isSensitive"])
		}
	}
	if !sawSensitive {
		t.Fatal("response did not include the seeded sensitive config")
	}
}

func TestAPI_Inspection_NoCommitsNoGroupJoinAfterEachRoute(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedBroker(1, "b1", 9092, "")
	gw.Fake.SeedTopic("t-demo", 1, kafka.Record{Value: []byte("v")})

	routes := []string{
		"/v1/cluster",
		"/v1/cluster/brokers/1/config",
		"/v1/cluster/health",
		"/v1/topics",
		"/v1/topics/t-demo",
		"/v1/topics/t-demo/size",
		"/v1/topics/t-demo/count?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z",
	}
	for _, route := range routes {
		resp := gw.Get(t, route)
		_ = resp.Body.Close()
	}

	gw.Fake.AssertNoCommits(t)
	gw.Fake.AssertNoGroupJoin(t)
}
