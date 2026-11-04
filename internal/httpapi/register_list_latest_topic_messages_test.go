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

type fakeReadTopicMessages struct {
	messages []topics.TopicMessage
	err      error
	topic    string
	position topics.TopicMessagePosition
}

func (f *fakeListLatestTopicMessages) Execute(_ context.Context, topic string) ([]topics.TopicMessage, error) {
	f.topic = topic
	return f.messages, f.err
}

func (f *fakeReadTopicMessages) Execute(_ context.Context, topic string, position topics.TopicMessagePosition) ([]topics.TopicMessage, error) {
	f.topic = topic
	f.position = position
	if position.Partition < 0 ||
		(position.Offset == nil) == (position.Timestamp == nil) ||
		(position.Offset != nil && *position.Offset < 0) ||
		(position.Timestamp != nil && *position.Timestamp < 0) {
		return nil, topics.ErrInvalidTopicMessagePosition
	}
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
	RegisterListLatestTopicMessages(api, action, &fakeReadTopicMessages{})

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
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{}, &fakeReadTopicMessages{})

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
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{err: topics.ErrTopicNotFound}, &fakeReadTopicMessages{})

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
	RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{err: errors.New("connection refused")}, &fakeReadTopicMessages{})

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

func TestRegisterListLatestTopicMessages_ReadsFromOffsetOrTimestamp(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		wantPosition topics.TopicMessagePosition
	}{
		{name: "offset", query: "partition=2&offset=12", wantPosition: topics.TopicMessagePosition{Partition: 2, Offset: new(int64)}},
		{name: "timestamp", query: "partition=1&timestamp=1700000000123", wantPosition: topics.TopicMessagePosition{Partition: 1, Timestamp: new(int64)}},
	}
	*tests[0].wantPosition.Offset = 12
	*tests[1].wantPosition.Timestamp = 1700000000123

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, api := humatest.New(t)
			read := &fakeReadTopicMessages{messages: []topics.TopicMessage{{Partition: tt.wantPosition.Partition, Offset: 12}}}
			latest := &fakeListLatestTopicMessages{}
			RegisterListLatestTopicMessages(api, latest, read)

			resp := api.Get("/api/v1/topics/orders/messages?" + tt.query)

			if resp.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
			}
			if read.topic != "orders" {
				t.Errorf("requested topic = %q, want orders", read.topic)
			}
			if diff := cmp.Diff(tt.wantPosition, read.position); diff != "" {
				t.Errorf("position mismatch (-want +got):\n%s", diff)
			}
			if latest.topic != "" {
				t.Error("latest action was called for a positioned read")
			}
		})
	}
}

func TestRegisterListLatestTopicMessages_Returns400ForInvalidPosition(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "partition missing", query: "offset=0"},
		{name: "both positions", query: "partition=0&offset=0&timestamp=1"},
		{name: "negative partition", query: "partition=-1&offset=0"},
		{name: "negative offset", query: "partition=0&offset=-1"},
		{name: "invalid number", query: "partition=0&offset=nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, api := humatest.New(t)
			RegisterListLatestTopicMessages(api, &fakeListLatestTopicMessages{}, &fakeReadTopicMessages{})

			resp := api.Get("/api/v1/topics/orders/messages?" + tt.query)

			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", resp.Code, resp.Body.String())
			}
			var got map[string]any
			if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got["code"] != "invalid_position" {
				t.Errorf("code = %v, want invalid_position", got["code"])
			}
		})
	}
}
