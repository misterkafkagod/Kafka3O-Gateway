package httpapi_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/gatewayproxy"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type fakeForwarder struct {
	requests []gatewayproxy.Request
	response gatewayproxy.Response
	err      error
}

func (f *fakeForwarder) Execute(_ context.Context, request gatewayproxy.Request) (gatewayproxy.Response, error) {
	f.requests = append(f.requests, request)
	return f.response, f.err
}

func TestGatewayForwardingRouteChecksAndMapsOutcomes(t *testing.T) {
	c := newTestConsole(t)
	enableTestClusterAdministration(t, c)
	manager, _ := session.NewManager(c.db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	api := c.handler.(*consoleapi.SessionAPI)
	forwarder := &fakeForwarder{response: gatewayproxy.Response{Status: 200, ContentType: "application/json", Body: []byte(`{"topics":[]}`)}}
	if err := api.EnableGatewayRequests(forwarder); err == nil {
		t.Fatal("forwarding mounted before the cluster list")
	}
	if err := api.EnableGatewayForwarding(c.db, manager); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableGatewayRequests(forwarder); err != nil {
		t.Fatal(err)
	}
	_, cookies, csrf := unrestrictedAdmin(t, c)
	base := "/api/clusters/dev/gateway"
	status, body := func() (int, string) {
		response, data := c.request(t, http.MethodGet, base+"/api/v1/topics?b=2&a=1", "", c.origin, cookies, "")
		return response.StatusCode, string(data)
	}()
	if status != 200 || body != `{"topics":[]}` || len(forwarder.requests) != 1 {
		t.Fatalf("read = %d %s", status, body)
	}
	got := forwarder.requests[0]
	if got.ClusterID != "dev" || got.GatewayURL != "https://dev.example:8081" || got.EscapedPath != "/api/v1/topics" ||
		got.Action != authorization.ActionListTopics || got.SessionID == "" || got.Query.Encode() != "a=1&b=2" {
		t.Fatalf("forwarded request = %+v", got)
	}
	for _, tt := range []struct {
		name, method, path, body, csrf string
		cookies                        bool
		status                         int
	}{
		{"anonymous", "GET", base + "/api/v1/topics", "", "", false, 401},
		{"write without CSRF", "POST", base + "/api/v1/topics/orders/messages", `{"value":"v"}`, "", true, 403},
		{"liveness not forwarded", "GET", base + "/api/v1/health/live", "", "", true, 404},
		{"unknown route", "GET", base + "/openapi.json", "", "", true, 404},
		{"unregistered cluster", "GET", "/api/clusters/nope/gateway/api/v1/topics", "", "", true, 404},
		{"duplicate query", "GET", base + "/api/v1/topics/orders/messages?limit=1&limit=2", "", "", true, 400},
		{"oversized body", "POST", base + "/api/v1/topics/orders/messages", strings.Repeat("x", (1<<20)+1), csrf, true, 413},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := len(forwarder.requests)
			jar := cookies
			if !tt.cookies {
				jar = nil
			}
			response, _ := c.request(t, tt.method, tt.path, tt.body, c.origin, jar, tt.csrf)
			if response.StatusCode != tt.status || len(forwarder.requests) != before {
				t.Fatalf("status=%d want %d forwarded=%d", response.StatusCode, tt.status, len(forwarder.requests)-before)
			}
		})
	}
	for _, tt := range []struct {
		name     string
		response gatewayproxy.Response
		err      error
		status   int
		code     string
	}{
		{"denied", gatewayproxy.Response{Status: 403, ContentType: "application/problem+json", Body: []byte(`{"status":403,"code":"forbidden","title":"Not permitted: insufficient_role"}`)}, nil, 403, "forbidden"},
		{"delegation rejected", gatewayproxy.Response{Status: 401, Body: []byte(`{"code":"invalid_delegation"}`)}, nil, 401, "unauthorized"},
		{"policy outage", gatewayproxy.Response{Status: 503, Body: []byte(`{"status":503,"code":"policy_unavailable"}`)}, nil, 503, "authorization_unavailable"},
		{"lost write response", gatewayproxy.Response{}, gatewayproxy.ErrOutcomeUnknown, 503, "gateway_outcome_unknown"},
		{"gateway down", gatewayproxy.Response{}, gatewayproxy.ErrGatewayUnavailable, 503, "gateway_unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			forwarder.response, forwarder.err = tt.response, tt.err
			_, cookies, csrf, _ := c.login(t, "admin", "replacement passphrase")
			response, data := c.request(t, http.MethodPost, base+"/api/v1/topics/orders/messages", `{"value":"v"}`, c.origin, cookies, csrf)
			if response.StatusCode != tt.status || !strings.Contains(string(data), `"code":"`+tt.code+`"`) {
				t.Fatalf("status=%d body=%s", response.StatusCode, data)
			}
		})
	}
}
