package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeProduceTopicMessage struct {
	calls   int
	topic   string
	request topics.ProduceMessageRequest
	result  topics.ProduceMessageResult
	err     error
}

func TestRegisterProduceTopicMessage_OpenAPISchema(t *testing.T) {
	_, api := humatest.New(t)
	RegisterProduceTopicMessage(api, &fakeProduceTopicMessage{})
	schemas := api.OpenAPI().Components.Schemas.Map()
	request := schemas["ProduceMessageRequest"]
	result := schemas["ProduceMessageResult"]
	if request == nil || result == nil || request.Properties["value"] == nil || result.Properties["offset"] == nil ||
		!slices.Contains(request.Required, "value") ||
		request.Properties["value"].Type != huma.TypeString || request.Properties["value"].Nullable ||
		result.Properties["offset"].Type != huma.TypeString {
		t.Fatalf("unexpected produce schemas: request = %#v, result = %#v", request, result)
	}
}

func (f *fakeProduceTopicMessage) Execute(_ context.Context, topic string, request topics.ProduceMessageRequest) (topics.ProduceMessageResult, error) {
	f.calls++
	f.topic = topic
	f.request = request
	return f.result, f.err
}

func TestRegisterProduceTopicMessage_ReturnsExactPosition(t *testing.T) {
	_, api := humatest.New(t)
	action := &fakeProduceTopicMessage{result: topics.ProduceMessageResult{Partition: 2, Offset: "9007199254740993"}}
	RegisterProduceTopicMessage(api, action)
	resp := api.Post("/api/v1/topics/orders/messages", map[string]any{
		"value": "", "key": "", "headers": []map[string]string{{"key": "source", "value": "test"}},
	})
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
	var result topics.ProduceMessageResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result != action.result || action.topic != "orders" || action.request.Value == nil || *action.request.Value != "" ||
		action.request.Key == nil || *action.request.Key != "" || len(action.request.Headers) != 1 {
		t.Fatalf("result = %#v, action = %#v", result, action)
	}
}

func TestRegisterProduceTopicMessage_RejectsInvalidInput(t *testing.T) {
	_, api := humatest.New(t)
	action := &fakeProduceTopicMessage{err: topics.ErrInvalidProduceMessage}
	RegisterProduceTopicMessage(api, action)
	resp := api.Post("/api/v1/topics/orders/messages", map[string]string{"value": "message"})
	if resp.Code != http.StatusBadRequest || !strings.Contains(resp.Body.String(), "invalid_message") {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
	for _, body := range []string{
		`{}`,
		`{"value":null}`,
		`{"value":"ok","headers":[{"key":"source"}]}`,
	} {
		resp = api.Post("/api/v1/topics/orders/messages", "Content-Type: application/json", strings.NewReader(body))
		if resp.Code < 400 || resp.Code >= 500 {
			t.Errorf("body %s: status = %d", body, resp.Code)
		}
	}
	action.calls = 0
	invalidUTF8 := append([]byte(`{"value":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	resp = api.Post("/api/v1/topics/orders/messages", "Content-Type: application/json", bytes.NewReader(invalidUTF8))
	if resp.Code != http.StatusBadRequest || action.calls != 0 {
		t.Errorf("invalid UTF-8: status = %d, calls = %d", resp.Code, action.calls)
	}
}

func TestRegisterProduceTopicMessage_ReportsUncertainDelivery(t *testing.T) {
	_, api := humatest.New(t)
	action := &fakeProduceTopicMessage{err: errors.New("internal broker error")}
	RegisterProduceTopicMessage(api, action)
	resp := api.Post("/api/v1/topics/orders/messages", map[string]string{"value": "message"})
	if resp.Code != http.StatusServiceUnavailable || !strings.Contains(resp.Body.String(), "delivery_unknown") ||
		!strings.Contains(resp.Body.String(), "may already have been written") || strings.Contains(resp.Body.String(), "internal broker error") {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
}
