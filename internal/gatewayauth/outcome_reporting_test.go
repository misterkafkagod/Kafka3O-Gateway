package gatewayauth_test

import (
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestMutationOutcomesAreRecordedBeforeTheResultIsReleased(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	serverIdentity := ca.Issue(t, "gateway-dev", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	policy := &fakePolicy{decision: gatewayauth.PolicyDecision{Allowed: true, OperationID: "op-7"}}
	status, reply := http.StatusCreated, `{"offset":42}`
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, reply)
	})
	handler, err := gatewayauth.WithDelegatedAuthorization(next, policy, "dev", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverIdentity.TLS}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: ca.Pool}
	server.StartTLS()
	defer server.Close()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{console.TLS}}}}
	send := func(method, path string) (int, string) {
		t.Helper()
		request, _ := http.NewRequest(method, server.URL+path, strings.NewReader(`{"value":"v"}`))
		request.Header.Set(gatewayauth.DelegationHeader, strings.Repeat("A", 43))
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}
	if code, body := send("POST", "/api/v1/topics/orders/messages"); code != http.StatusCreated || body != `{"offset":42}` ||
		len(policy.outcomes) != 1 || policy.outcomes[0] != "op-7/confirmed" {
		t.Fatalf("confirmed mutation = %d %s outcomes=%v", code, body, policy.outcomes)
	}
	status, reply = http.StatusServiceUnavailable, `{"status":503,"code":"delivery_unknown"}`
	if code, _ := send("POST", "/api/v1/topics/orders/messages"); code != http.StatusServiceUnavailable || policy.outcomes[1] != "op-7/unknown" {
		t.Fatalf("unknown delivery = %d outcomes=%v", code, policy.outcomes)
	}
	status, reply = http.StatusOK, `{"offset":43}`
	policy.outcomeErr = errors.New("audit store down")
	code, body := send("POST", "/api/v1/topics/orders/messages")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"outcome_unknown"`) || !strings.Contains(body, `"operationId":"op-7"`) ||
		strings.Contains(body, "offset") {
		t.Fatalf("unrecorded outcome = %d %s", code, body)
	}
	recorded := len(policy.outcomes)
	if code, body := send("GET", "/api/v1/topics"); code != http.StatusOK || body != `{"offset":43}` || len(policy.outcomes) != recorded {
		t.Fatalf("read reported an outcome or was blocked: %d %s outcomes=%v", code, body, policy.outcomes)
	}
}
