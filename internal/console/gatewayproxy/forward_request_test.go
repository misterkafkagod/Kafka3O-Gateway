package gatewayproxy_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

type fakeMint struct {
	binding [32]byte
	action  authorization.Action
	calls   int
}

func (f *fakeMint) Execute(_ context.Context, _, _ string, action authorization.Action, _ string, binding [32]byte, _ string) (string, error) {
	f.calls++
	f.binding, f.action = binding, action
	return "handle-from-mint", nil
}

func TestForwardSendsExactBytesOnceToThePinnedGateway(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	start := func(uri string, handler http.HandlerFunc) *httptest.Server {
		identity := ca.Issue(t, "gw", uri, time.Now().Add(time.Hour))
		server := httptest.NewUnstartedServer(handler)
		server.TLS = &tls.Config{Certificates: []tls.Certificate{identity.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
		server.StartTLS()
		t.Cleanup(server.Close)
		return server
	}
	var calls int
	var received, header, rawQuery string
	gateway := start(servicetls.GatewayURIPrefix+"dev", func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		received, header, rawQuery = string(body), r.Header.Get(gatewayauth.DelegationHeader), r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"offset":1}`))
	})
	mint := &fakeMint{}
	forward, err := gatewayproxy.NewForwardRequest(mint, console.TLS, ca.Pool)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"value": "exact  bytes"}`)
	query := url.Values{"b": {"2"}, "a": {"1"}}
	response, err := forward.Execute(context.Background(), gatewayproxy.Request{SessionID: "s", ClusterID: "dev", GatewayURL: gateway.URL,
		Method: "POST", EscapedPath: "/api/v1/topics/orders/messages", Query: query, Body: body, Action: authorization.ActionProduceMessage})
	if err != nil || response.Status != http.StatusCreated || string(response.Body) != `{"offset":1}` || calls != 1 {
		t.Fatalf("response = %+v %v calls=%d", response, err, calls)
	}
	if received != string(body) || header != "handle-from-mint" || rawQuery != "a=1&b=2" || mint.action != authorization.ActionProduceMessage ||
		mint.binding != authorization.RequestBinding("dev", "POST", "/api/v1/topics/orders/messages", query, body) {
		t.Fatalf("gateway saw body=%q header=%q query=%q", received, header, rawQuery)
	}
	impostor := start(servicetls.GatewayURIPrefix+"prod", func(w http.ResponseWriter, r *http.Request) { calls++ })
	if _, err := forward.Execute(context.Background(), gatewayproxy.Request{SessionID: "s", ClusterID: "dev", GatewayURL: impostor.URL,
		Method: "DELETE", EscapedPath: "/api/v1/topics/orders", Action: authorization.ActionDeleteTopic}); !errors.Is(err, gatewayproxy.ErrOutcomeUnknown) || calls != 1 {
		t.Fatalf("another cluster's Gateway was trusted: %v calls=%d", err, calls)
	}
	impostor.Close()
	if _, err := forward.Execute(context.Background(), gatewayproxy.Request{SessionID: "s", ClusterID: "dev", GatewayURL: "https://127.0.0.1:1",
		Method: "GET", EscapedPath: "/api/v1/topics", Action: authorization.ActionListTopics}); !errors.Is(err, gatewayproxy.ErrGatewayUnavailable) {
		t.Fatalf("unreachable read error = %v", err)
	}
	if _, err := forward.Execute(context.Background(), gatewayproxy.Request{SessionID: "s", ClusterID: "dev", GatewayURL: gateway.URL,
		Method: "GET", EscapedPath: "/openapi.json", Action: authorization.ActionListTopics}); !errors.Is(err, gatewayproxy.ErrConfig) {
		t.Fatalf("non-API path error = %v", err)
	}
}
