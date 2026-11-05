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

type fakeDeleteKafkaTopic struct {
	calls int
	name  string
	err   error
}

func (f *fakeDeleteKafkaTopic) Execute(_ context.Context, name string) error {
	f.calls++
	f.name = name
	return f.err
}

func TestRegisterDeleteTopic_ReportsAcceptedRequest(t *testing.T) {
	_, api := humatest.New(t)
	adapter := &fakeDeleteKafkaTopic{}
	RegisterDeleteTopic(api, topics.NewDeleteTopic(adapter, time.Second))
	resp := api.Delete("/api/v1/topics/orders")
	if resp.Code != http.StatusAccepted || !strings.Contains(resp.Body.String(), `"status":"accepted"`) ||
		adapter.calls != 1 || adapter.name != "orders" {
		t.Fatalf("status = %d, body = %s, adapter = %#v", resp.Code, resp.Body.String(), adapter)
	}
	resp = api.Delete("/api/v1/topics/bad%20name")
	if resp.Code != http.StatusBadRequest || adapter.calls != 1 {
		t.Fatalf("invalid status = %d, calls = %d, body = %s", resp.Code, adapter.calls, resp.Body.String())
	}
}

func TestRegisterDeleteTopic_MapsMissingAndUnconfirmedOutcomes(t *testing.T) {
	_, api := humatest.New(t)
	adapter := &fakeDeleteKafkaTopic{err: topics.ErrTopicNotFound}
	RegisterDeleteTopic(api, topics.NewDeleteTopic(adapter, time.Second))
	resp := api.Delete("/api/v1/topics/orders")
	if resp.Code != http.StatusNotFound || !strings.Contains(resp.Body.String(), "not_found") {
		t.Fatalf("missing status = %d, body = %s", resp.Code, resp.Body.String())
	}
	adapter.err = errors.New("broker response lost")
	resp = api.Delete("/api/v1/topics/orders")
	if resp.Code != http.StatusServiceUnavailable || !strings.Contains(resp.Body.String(), "topic_write_unknown") ||
		!strings.Contains(resp.Body.String(), "may already have succeeded") || strings.Contains(resp.Body.String(), "broker response lost") {
		t.Fatalf("unknown status = %d, body = %s", resp.Code, resp.Body.String())
	}
}
