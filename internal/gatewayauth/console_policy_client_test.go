package gatewayauth_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestConsolePolicyClientMapsResponsesAndPinsTheConsole(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	impostor := ca.Issue(t, "impostor", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	gateway := ca.Issue(t, "gateway-prod", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))
	status := http.StatusOK
	body := `{"operationId":"op","allowed":true,"actorId":"alice","source":"local","denial":""}`
	var seen map[string]string
	start := func(identity servicetlstest.Identity) *httptest.Server {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cluster, _ := servicetls.GatewayCluster(r.TLS)
			_ = json.NewDecoder(r.Body).Decode(&seen)
			seen["cluster"] = cluster
			seen["path"] = r.URL.Path
			if status == http.StatusFound {
				http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
				return
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{identity.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
		server.StartTLS()
		t.Cleanup(server.Close)
		return server
	}
	server := start(console)
	client, err := gatewayauth.NewConsolePolicyClient(server.URL, gateway.TLS, ca.Pool)
	if err != nil {
		t.Fatal(err)
	}
	binding := [32]byte{1, 2, 3}
	decision, err := client.Consume(context.Background(), "handle", binding, authorization.ActionDeleteTopic)
	if err != nil || !decision.Allowed || decision.ActorID != "alice" || seen["cluster"] != "prod" ||
		seen["path"] != "/internal/v1/delegations/consume" || seen["action"] != "topics.delete" || seen["handle"] != "handle" {
		t.Fatalf("decision = %+v %v seen=%v", decision, err, seen)
	}
	for _, tt := range []struct {
		status int
		body   string
		want   error
	}{
		{http.StatusUnauthorized, `{"code":"invalid_delegation"}`, gatewayauth.ErrInvalidDelegation},
		{http.StatusServiceUnavailable, `{"code":"policy_unavailable"}`, gatewayauth.ErrPolicyUnavailable},
		{http.StatusForbidden, `{"code":"unknown_gateway"}`, gatewayauth.ErrPolicyUnavailable},
		{http.StatusOK, `{"operationId":"op"}`, gatewayauth.ErrPolicyUnavailable},
		{http.StatusFound, ``, gatewayauth.ErrPolicyUnavailable},
	} {
		status, body = tt.status, tt.body
		if _, err := client.Consume(context.Background(), "handle", binding, authorization.ActionDeleteTopic); !errors.Is(err, tt.want) {
			t.Fatalf("status %d error = %v, want %v", tt.status, err, tt.want)
		}
	}
	impostorServer := start(impostor)
	impostorClient, _ := gatewayauth.NewConsolePolicyClient(impostorServer.URL, gateway.TLS, ca.Pool)
	status, body = http.StatusOK, `{"allowed":true}`
	if _, err := impostorClient.Consume(context.Background(), "handle", binding, authorization.ActionDeleteTopic); !errors.Is(err, gatewayauth.ErrPolicyUnavailable) {
		t.Fatalf("a non-Console policy server was trusted: %v", err)
	}
}
