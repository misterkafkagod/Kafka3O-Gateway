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

type fakeSearchTopicMessages struct {
	result topics.TopicMessageSearchResult
	err    error
	topic  string
	query  string
}

func (f *fakeSearchTopicMessages) Execute(_ context.Context, topic, query string) (topics.TopicMessageSearchResult, error) {
	f.topic, f.query = topic, query
	if query == "" {
		return topics.TopicMessageSearchResult{}, topics.ErrInvalidTopicMessageSearch
	}
	return f.result, f.err
}

func TestRegisterSearchTopicMessages_ReturnsSearchResult(t *testing.T) {
	_, api := humatest.New(t)
	want := topics.TopicMessageSearchResult{
		Messages:  []topics.TopicMessage{{Partition: 1, Offset: 9, Value: []byte("order created")}},
		Scanned:   500,
		Truncated: true,
	}
	search := &fakeSearchTopicMessages{result: want}
	RegisterSearchTopicMessages(api, search)

	resp := api.Get("/api/v1/topics/orders/messages/search?query=order%20created")
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var got topics.TopicMessageSearchResult
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("search result mismatch (-want +got):\n%s", diff)
	}
	if search.topic != "orders" || search.query != "order created" {
		t.Errorf("request = %q, %q", search.topic, search.query)
	}
}

func TestRegisterSearchTopicMessages_ReturnsEmptyArray(t *testing.T) {
	_, api := humatest.New(t)
	RegisterSearchTopicMessages(api, &fakeSearchTopicMessages{})
	resp := api.Get("/api/v1/topics/orders/messages/search?query=x")
	if resp.Code != http.StatusOK || resp.Body.String() != "{\"messages\":[],\"scanned\":0,\"truncated\":false}\n" {
		t.Errorf("response = %d %q", resp.Code, resp.Body.String())
	}
}

func TestRegisterSearchTopicMessages_ReturnsProblemErrors(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		err    error
		status int
		code   string
	}{
		{name: "empty query", query: "", status: http.StatusBadRequest, code: "invalid_search"},
		{name: "missing topic", query: "?query=x", err: topics.ErrTopicNotFound, status: http.StatusNotFound, code: "not_found"},
		{name: "Kafka unavailable", query: "?query=x", err: errors.New("offline"), status: http.StatusServiceUnavailable, code: "cluster_unreachable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, api := humatest.New(t)
			RegisterSearchTopicMessages(api, &fakeSearchTopicMessages{err: tt.err})
			resp := api.Get("/api/v1/topics/orders/messages/search" + tt.query)
			if resp.Code != tt.status {
				t.Fatalf("status = %d, want %d: %s", resp.Code, tt.status, resp.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["code"] != tt.code {
				t.Errorf("problem code = %v, want %q", body["code"], tt.code)
			}
		})
	}
}
