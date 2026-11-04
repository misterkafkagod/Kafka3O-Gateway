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

type fakeGetTopicDetail struct {
	detail topics.TopicDetail
	err    error
	name   string
}

func (f *fakeGetTopicDetail) Execute(_ context.Context, name string) (topics.TopicDetail, error) {
	f.name = name
	return f.detail, f.err
}

func TestRegisterGetTopicDetail_ReturnsTopicDetails(t *testing.T) {
	_, api := humatest.New(t)
	want := topics.TopicDetail{
		Name: "orders",
		Partitions: []topics.TopicPartition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, StartOffset: 4, EndOffset: 9},
		},
		Config: []topics.TopicConfig{
			{Name: "cleanup.policy", Value: "compact"},
			{Name: "secret", Sensitive: true},
		},
	}
	action := &fakeGetTopicDetail{detail: want}
	RegisterGetTopicDetail(api, action)

	resp := api.Get("/api/v1/topics/orders")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var got topics.TopicDetail
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("detail mismatch (-want +got):\n%s", diff)
	}
	if action.name != "orders" {
		t.Errorf("requested topic = %q, want orders", action.name)
	}
}

func TestRegisterGetTopicDetail_Returns404ForMissingTopic(t *testing.T) {
	_, api := humatest.New(t)
	RegisterGetTopicDetail(api, &fakeGetTopicDetail{err: topics.ErrTopicNotFound})

	resp := api.Get("/api/v1/topics/missing")

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["code"] != "not_found" {
		t.Errorf("code = %v, want not_found", got["code"])
	}
}

func TestRegisterGetTopicDetail_Returns503OnKafkaError(t *testing.T) {
	_, api := humatest.New(t)
	RegisterGetTopicDetail(api, &fakeGetTopicDetail{err: errors.New("connection refused")})

	resp := api.Get("/api/v1/topics/orders")

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
