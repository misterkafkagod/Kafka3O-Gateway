package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeCreateTopicForServer struct{ calls int }

func (f *fakeCreateTopicForServer) Execute(_ context.Context, _ topics.CreateTopicRequest) error {
	f.calls++
	return nil
}

type fakeDeleteTopicForServer struct{ calls int }

func (f *fakeDeleteTopicForServer) Execute(_ context.Context, _ string) error {
	f.calls++
	return nil
}

type fakeProduceTopicForServer struct{ calls int }

func (f *fakeProduceTopicForServer) Execute(_ context.Context, _ string, _ topics.ProduceMessageRequest) (topics.ProduceMessageResult, error) {
	f.calls++
	return topics.ProduceMessageResult{Partition: 0, Offset: "0"}, nil
}

func TestNewGatewayHTTPServer_ProtectsWriteRoutes(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	fixture.Close()
	token := strings.Repeat("a", 32)
	mux := http.NewServeMux()
	api := humago.New(mux, huma.DefaultConfig("test", "1.0"))
	create, remove, produce := &fakeCreateTopicForServer{}, &fakeDeleteTopicForServer{}, &fakeProduceTopicForServer{}
	httpapi.RegisterCreateTopic(api, create)
	httpapi.RegisterDeleteTopic(api, remove)
	httpapi.RegisterProduceTopicMessage(api, produce)
	gateway := newGatewayHTTPServer("", mux, []string{"https://ui.example"}, token, certificate)
	server := httptest.NewUnstartedServer(gateway.Handler)
	server.TLS = gateway.TLSConfig
	server.StartTLS()
	defer server.Close()
	client := server.Client()

	for _, tt := range []struct {
		method, path, body string
		status             int
	}{
		{http.MethodPost, "/api/v1/topics", `{"name":"orders","partitions":1,"replicationFactor":1}`, http.StatusCreated},
		{http.MethodPost, "/api/v1/topics/orders/messages", `{"value":""}`, http.StatusOK},
		{http.MethodDelete, "/api/v1/topics/orders", "", http.StatusAccepted},
	} {
		for _, authorized := range []bool{false, true} {
			request, err := http.NewRequest(tt.method, server.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if tt.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			if authorized {
				request.Header.Set("Authorization", "Bearer "+token)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			want := http.StatusUnauthorized
			if authorized {
				want = tt.status
			}
			if response.StatusCode != want {
				t.Errorf("%s %s authorized=%v: got %d, want %d", tt.method, tt.path, authorized, response.StatusCode, want)
			}
		}
	}
	if create.calls != 1 || remove.calls != 1 || produce.calls != 1 {
		t.Errorf("authorized action calls = create:%d delete:%d produce:%d", create.calls, remove.calls, produce.calls)
	}
}
