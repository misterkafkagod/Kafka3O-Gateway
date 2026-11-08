package gatewayauth_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

type fakePolicy struct {
	decision gatewayauth.PolicyDecision
	err      error
	delay    time.Duration
	calls    atomic.Int64
	handle   string
	binding  [32]byte
	action   authorization.Action
}

func (f *fakePolicy) Consume(ctx context.Context, handle string, binding [32]byte, action authorization.Action) (gatewayauth.PolicyDecision, error) {
	f.calls.Add(1)
	f.handle, f.binding, f.action = handle, binding, action
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return gatewayauth.PolicyDecision{}, ctx.Err()
		}
	}
	return f.decision, f.err
}

func TestDelegatedAuthorizationAdmitsOnlyAcceptedConsoleRequests(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	other := ca.Issue(t, "gateway-dev", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	server := ca.Issue(t, "gateway-prod", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))
	policy := &fakePolicy{decision: gatewayauth.PolicyDecision{Allowed: true}}
	var kafkaCalls atomic.Int64
	var seenBody string
	var seenHeader string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kafkaCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		seenBody, seenHeader = string(body), r.Header.Get(gatewayauth.DelegationHeader)
		w.WriteHeader(http.StatusOK)
	})
	handler, err := gatewayauth.WithDelegatedAuthorization(next, policy, "prod", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewUnstartedServer(handler)
	gateway.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{server.TLS}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: ca.Pool}
	gateway.StartTLS()
	defer gateway.Close()
	send := func(identity *servicetlstest.Identity, method, path, body string, handles ...string) (int, string) {
		t.Helper()
		config := &tls.Config{RootCAs: ca.Pool}
		if identity != nil {
			config.Certificates = []tls.Certificate{identity.TLS}
		}
		request, _ := http.NewRequest(method, gateway.URL+path, strings.NewReader(body))
		for _, handle := range handles {
			request.Header.Add(gatewayauth.DelegationHeader, handle)
		}
		response, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: config}}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(data)
	}
	handle := strings.Repeat("A", 43)
	produce := `{"value":"v"}`
	if status, _ := send(nil, "GET", "/api/v1/health/live", ""); status != 200 {
		t.Fatalf("anonymous liveness = %d", status)
	}
	for _, tt := range []struct {
		name     string
		identity *servicetlstest.Identity
		method   string
		path     string
		body     string
		handles  []string
		status   int
		code     string
	}{
		{"no client certificate", nil, "POST", "/api/v1/topics/orders/messages", produce, []string{handle}, 401, "service_authentication_required"},
		{"another service", &other, "POST", "/api/v1/topics/orders/messages", produce, []string{handle}, 401, "service_authentication_required"},
		{"unknown route", &console, "GET", "/openapi.json", "", []string{handle}, 403, "action_not_classified"},
		{"missing delegation", &console, "POST", "/api/v1/topics/orders/messages", produce, nil, 401, "delegation_required"},
		{"two delegations", &console, "POST", "/api/v1/topics/orders/messages", produce, []string{handle, handle}, 401, "delegation_required"},
		{"duplicate query key", &console, "GET", "/api/v1/topics/orders/messages?limit=1&limit=2", "", []string{handle}, 400, "invalid_request"},
		{"oversized body", &console, "POST", "/api/v1/topics/orders/messages", strings.Repeat("x", gatewayauth.MaxBodyBytes+1), []string{handle}, 413, "request_too_large"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := policy.calls.Load()
			status, body := send(tt.identity, tt.method, tt.path, tt.body, tt.handles...)
			if status != tt.status || !strings.Contains(body, `"code":"`+tt.code+`"`) || policy.calls.Load() != before {
				t.Fatalf("status=%d body=%s policyCalls=%d", status, body, policy.calls.Load()-before)
			}
		})
	}
	if status, _ := send(&console, "POST", "/api/v1/topics/orders/messages", produce, handle); status != 200 || seenBody != produce || seenHeader != "" {
		t.Fatalf("allowed request: status=%d body=%q header=%q", status, seenBody, seenHeader)
	}
	if policy.action != authorization.ActionProduceMessage || policy.handle != handle ||
		policy.binding != authorization.RequestBinding("prod", "POST", "/api/v1/topics/orders/messages", nil, []byte(produce)) {
		t.Fatalf("policy saw action=%s handle=%q or a wrong binding", policy.action, policy.handle)
	}
	for _, tt := range []struct {
		name   string
		setup  func()
		status int
		code   string
	}{
		{"denied", func() {
			policy.decision, policy.err = gatewayauth.PolicyDecision{Denial: "production_admin_required"}, nil
		}, 403, "forbidden"},
		{"invalid", func() { policy.err = gatewayauth.ErrInvalidDelegation }, 401, "invalid_delegation"},
		{"unavailable", func() { policy.err = errors.Join(gatewayauth.ErrPolicyUnavailable, errors.New("down")) }, 503, "policy_unavailable"},
		{"slow", func() {
			policy.err, policy.delay, policy.decision = nil, 3*time.Second, gatewayauth.PolicyDecision{Allowed: true}
		}, 503, "policy_unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			before := kafkaCalls.Load()
			start := time.Now()
			status, body := send(&console, "DELETE", "/api/v1/topics/orders", "", handle)
			if status != tt.status || !strings.Contains(body, `"code":"`+tt.code+`"`) || kafkaCalls.Load() != before {
				t.Fatalf("status=%d body=%s kafkaCalls=%d", status, body, kafkaCalls.Load()-before)
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("policy budget exceeded: %v", elapsed)
			}
		})
	}
	if bytes.Contains([]byte(seenBody), []byte("delegation")) {
		t.Fatal("unexpected body")
	}
}
