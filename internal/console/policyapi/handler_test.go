package policyapi_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/policyapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

type fakeConsume struct {
	requests []delegation.ConsumeRequest
	decision delegation.Decision
	err      error
}

func (f *fakeConsume) Execute(_ context.Context, request delegation.ConsumeRequest) (delegation.Decision, error) {
	f.requests = append(f.requests, request)
	return f.decision, f.err
}

type fakeClusters map[string]bool

func (f fakeClusters) ActiveRegisteredCluster(_ context.Context, id string) (bool, error) {
	if id == "broken" {
		return false, errors.New("store down")
	}
	return f[id], nil
}

func TestPolicyHandlerAuthenticatesGatewaysAndMapsDecisions(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	consume := &fakeConsume{decision: delegation.Decision{OperationID: "op", Allowed: true, ActorID: "alice", Source: "local"}}
	var logs bytes.Buffer
	handler, err := policyapi.NewHandler(consume, fakeClusters{"prod": true, "old": false}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{console.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
	server.StartTLS()
	defer server.Close()
	binding := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	valid := `{"handle":"` + strings.Repeat("A", 43) + `","binding":"` + binding + `","action":"messages.produce"}`
	post := func(cluster, path, body string) (int, string) {
		t.Helper()
		identity := ca.Issue(t, "gateway-"+cluster, servicetls.GatewayURIPrefix+cluster, time.Now().Add(time.Hour))
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{identity.TLS}}}}
		response, err := client.Post(server.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("policy response is cacheable")
		}
		return response.StatusCode, string(data)
	}
	if status, body := post("prod", policyapi.ConsumePath, valid); status != 200 || !strings.Contains(body, `"allowed":true`) || !strings.Contains(body, `"actorId":"alice"`) {
		t.Fatalf("allowed decision = %d %s", status, body)
	}
	if got := consume.requests[0]; got.CallerClusterID != "prod" || got.Action != authorization.ActionProduceMessage || got.Handle != strings.Repeat("A", 43) {
		t.Fatalf("consume request = %+v", got)
	}
	for _, tt := range []struct {
		name, cluster, path, body string
		status                    int
	}{
		{"inactive cluster", "old", policyapi.ConsumePath, valid, http.StatusForbidden},
		{"unregistered cluster", "nope", policyapi.ConsumePath, valid, http.StatusForbidden},
		{"registry failure", "broken", policyapi.ConsumePath, valid, http.StatusServiceUnavailable},
		{"wrong path", "prod", "/internal/v1/other", valid, http.StatusNotFound},
		{"query string", "prod", policyapi.ConsumePath + "?cluster=dev", valid, http.StatusNotFound},
		{"caller names a cluster", "prod", policyapi.ConsumePath, strings.TrimSuffix(valid, "}") + `,"clusterId":"dev"}`, http.StatusBadRequest},
		{"short binding", "prod", policyapi.ConsumePath, `{"handle":"h","binding":"AAAA","action":"x"}`, http.StatusBadRequest},
		{"trailing data", "prod", policyapi.ConsumePath, valid + "{}", http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := len(consume.requests)
			if status, _ := post(tt.cluster, tt.path, tt.body); status != tt.status {
				t.Fatalf("status = %d, want %d", status, tt.status)
			}
			if len(consume.requests) != before {
				t.Fatal("a rejected request reached the decision")
			}
		})
	}
	consume.err = delegation.ErrInvalidDelegation
	if status, body := post("prod", policyapi.ConsumePath, valid); status != http.StatusUnauthorized || !strings.Contains(body, "invalid_delegation") {
		t.Fatalf("invalid delegation = %d %s", status, body)
	}
	consume.err = errors.Join(delegation.ErrPolicyUnavailable, errors.New("secret-internal-detail"))
	if status, body := post("prod", policyapi.ConsumePath, valid); status != http.StatusServiceUnavailable || strings.Contains(body, "secret") {
		t.Fatalf("unavailable = %d %s", status, body)
	}
	if strings.Contains(logs.String(), "secret-internal-detail") || strings.Contains(logs.String(), strings.Repeat("A", 43)) {
		t.Fatal("policy logs exposed an error message or handle")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool}}}
	if _, err := client.Post(server.URL+policyapi.ConsumePath, "application/json", strings.NewReader(valid)); err == nil {
		t.Fatal("a caller without a client certificate was served")
	}
}
