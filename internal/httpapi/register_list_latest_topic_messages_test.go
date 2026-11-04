package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/google/go-cmp/cmp"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeListLatestTopicMessages struct {
	messages []topics.TopicMessage
	err      error
	topic    string
}

func (f *fakeListLatestTopicMessages) Execute(_ context.Context, topic string) ([]topics.TopicMessage, error) {
	f.topic = topic
	return f.messages, f.err
}

func TestRegisterListLatestTopicMessages_ReturnsMessages(t *testing.T) {
	_, api := humatest.New(t)
	want := []topics.TopicMessage{{
		Partition: 1,
		Offset:    12,
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Key:       []byte("order-1"),
		Value:     []byte("created"),
		Headers:   []topics.TopicMessageHeader{{Key: "source", Value: []byte("api")}},
	}}
	action := &fakeListLatestTopicMessages{messages: want}
	RegisterListLatestTopicMessages(api, action)

	resp := api.Get("/api/v1/topics/orders/messages")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var got []topics.TopicMessage
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
	if action.topic != "orders" {
		t.Errorf("requested topic = %q, want orders", action.topic)
	}
}

func TestRegisterListLatestTopicMessages_ReturnsEmptyArray(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{})

	resp := api.Get("/api/v1/topics/orders/messages")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	if got := resp.Body.String(); got != "[]\n" {
		t.Errorf("body = %q, want empty JSON array", got)
	}
}

func TestRegisterListLatestTopicMessages_Returns404ForMissingTopic(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{err: topics.ErrTopicNotFound})

	resp := api.Get("/api/v1/topics/missing/messages")

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

func TestRegisterListLatestTopicMessages_Returns503OnKafkaError(t *testing.T) {
	_, api := humatest.New(t)
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{err: errors.New("connection refused")})

	resp := api.Get("/api/v1/topics/orders/messages")

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
