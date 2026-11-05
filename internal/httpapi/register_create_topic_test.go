package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeCreateKafkaTopic struct {
	calls   int
	request topics.CreateTopicRequest
	err     error
}

func (f *fakeCreateKafkaTopic) Execute(_ context.Context, request topics.CreateTopicRequest) error {
	f.calls++
	f.request = request
	return f.err
}

func TestRegisterCreateTopic_CreatesAndRejectsInvalidInput(t *testing.T) {
	_, api := humatest.New(t)
	adapter := &fakeCreateKafkaTopic{}
	RegisterCreateTopic(api, topics.NewCreateTopic(adapter, time.Second))
	resp := api.Post("/api/v1/topics", map[string]any{"name": "orders", "partitions": 3, "replicationFactor": 2})
	if resp.Code != http.StatusCreated || adapter.calls != 1 || adapter.request != (topics.CreateTopicRequest{Name: "orders", Partitions: 3, ReplicationFactor: 2}) {
		t.Fatalf("status = %d, body = %s, adapter = %#v", resp.Code, resp.Body.String(), adapter)
	}
	for _, body := range []string{
		`{"name":"..","partitions":1,"replicationFactor":1}`,
		`{"name":"orders","partitions":0,"replicationFactor":1}`,
		`{"name":"orders","partitions":1}`,
		`{"name":"orders","partitions":2147483648,"replicationFactor":1}`,
		`{"name":"orders","partitions":1,"replicationFactor":32768}`,
	} {
		resp = api.Post("/api/v1/topics", "Content-Type: application/json", strings.NewReader(body))
		if resp.Code < 400 || resp.Code >= 500 || adapter.calls != 1 {
			t.Errorf("body %s: status = %d, calls = %d", body, resp.Code, adapter.calls)
		}
	}
}

func TestRegisterCreateTopic_MapsConflictAndUnknownOutcome(t *testing.T) {
	_, api := humatest.New(t)
	adapter := &fakeCreateKafkaTopic{err: topics.ErrTopicAlreadyExists}
	RegisterCreateTopic(api, topics.NewCreateTopic(adapter, time.Second))
	body := map[string]any{"name": "orders", "partitions": 1, "replicationFactor": 1}
	resp := api.Post("/api/v1/topics", body)
	if resp.Code != http.StatusConflict || !strings.Contains(resp.Body.String(), "topic_exists") {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
	adapter.err = errors.New("broker response lost")
	resp = api.Post("/api/v1/topics", body)
	if resp.Code != http.StatusServiceUnavailable || !strings.Contains(resp.Body.String(), "topic_write_unknown") ||
		!strings.Contains(resp.Body.String(), "may already have succeeded") || strings.Contains(resp.Body.String(), "broker response lost") {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
}
