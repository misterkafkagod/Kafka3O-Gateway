package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/google/go-cmp/cmp"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeListTopics struct {
	topics []topics.Topic
	err    error
}

func (f fakeListTopics) Execute(context.Context) ([]topics.Topic, error) {
	return f.topics, f.err
}

func TestRegisterListTopics_ReturnsTopicsJSON(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListTopics(api, fakeListTopics{topics: []topics.Topic{
		{Name: "audit", Partitions: 1, ReplicationFactor: 1},
		{Name: "orders", Partitions: 6, ReplicationFactor: 3, ApproxMessages: 1000},
	}})

	resp := api.Get("/api/v1/topics")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	var got []map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"name": "audit", "partitions": float64(1), "replicationFactor": float64(1), "internal": false, "approxMessages": float64(0)},
		{"name": "orders", "partitions": float64(6), "replicationFactor": float64(3), "internal": false, "approxMessages": float64(1000)},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("body mismatch (-want +got):\n%s", diff)
	}
}

func TestRegisterListTopics_ReturnsEmptyArrayForNoTopics(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListTopics(api, fakeListTopics{topics: []topics.Topic{}})

	resp := api.Get("/api/v1/topics")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if got := resp.Body.String(); got != "[]\n" && got != "[]" {
		t.Errorf("body = %q, want []", got)
	}
}

func TestRegisterListTopics_Returns503ClusterUnreachableOnError(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListTopics(api, fakeListTopics{err: errors.New("dial tcp: connection refused")})

	resp := api.Get("/api/v1/topics")

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["code"] != "cluster_unreachable" {
		t.Errorf("code = %v, want cluster_unreachable", got["code"])
	}
}
