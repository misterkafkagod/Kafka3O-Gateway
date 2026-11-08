package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/delegation"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/policyapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/gatewayauth"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

type recordedKafkaCall struct {
	cluster, method, path, body string
}

// TestSecuredForwardingEndToEnd drives the real Console session API, forwarder and policy listener against
// secured Gateway handlers whose "Kafka" actions only record calls.
func TestSecuredForwardingEndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ca := servicetlstest.NewAuthority(t)
	consoleIdentity := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	db, err := storage.OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	consume, _ := delegation.NewConsumeDelegation(db, db, db, nil, rand.Reader, time.Now)
	outcome, _ := delegation.NewRecordOutcome(db, time.Now)
	policyHandler, _ := policyapi.NewHandler(consume, outcome, db, logger)
	policy := httptest.NewUnstartedServer(policyHandler)
	policy.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{consoleIdentity.TLS},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
	policy.StartTLS()
	defer policy.Close()

	var mu sync.Mutex
	var calls []recordedKafkaCall
	gateways := map[string]*httptest.Server{}
	for _, cluster := range []string{"dev", "prod"} {
		identity := ca.Issue(t, "gateway-"+cluster, servicetls.GatewayURIPrefix+cluster, time.Now().Add(time.Hour))
		client, _ := gatewayauth.NewConsolePolicyClient(policy.URL, identity.TLS, ca.Pool)
		kafka := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			calls = append(calls, recordedKafkaCall{cluster, r.Method, r.URL.Path, string(body)})
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		})
		handler, _ := gatewayauth.WithDelegatedAuthorization(kafka, client, cluster, logger)
		server := httptest.NewUnstartedServer(handler)
		server.Config = newSecuredGatewayServerForTest(handler, identity.TLS, ca)
		server.TLS = server.Config.TLSConfig
		server.StartTLS()
		defer server.Close()
		gateways[cluster] = server
	}

	consoleServer := httptest.NewUnstartedServer(nil)
	origin := "https://" + consoleServer.Listener.Addr().String()
	api, err := newSessionHandler(db, origin, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := enableGatewayRequests(api, db, forwardingConfig{identity: consoleIdentity.TLS, serviceCA: ca.Pool}); err != nil {
		t.Fatal(err)
	}
	consoleServer.Config.Handler = api
	consoleServer.StartTLS()
	defer consoleServer.Close()
	browser := consoleServer.Client()

	// Accounts, grants, registry and classification through the public services.
	client := netip.MustParseAddr("192.0.2.20")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{6}, 32))
	bootstrap, _ := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	authenticate, _ := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	change, _ := identity.NewChangeLocalPassword(authenticate, db, rand.Reader, time.Now)
	sessions, _ := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	signIn := func(username, temporary string) session.Credentials {
		t.Helper()
		account, err := authenticate.Execute(ctx, username, temporary, client)
		if err != nil {
			t.Fatal(err)
		}
		if err := change.Execute(ctx, account.ID, username, temporary, "replacement passphrase", client); err != nil {
			t.Fatal(err)
		}
		account, _ = authenticate.Execute(ctx, username, "replacement passphrase", client)
		credentials, err := sessions.IssueLocal(ctx, account)
		if err != nil {
			t.Fatal(err)
		}
		return credentials
	}
	if _, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase"); err != nil {
		t.Fatal(err)
	}
	adminCredentials := signIn("admin", "initial passphrase")
	admin := identity.LocalAdminActor{AccountID: adminCredentials.Principal.AccountID, SessionID: adminCredentials.Principal.SessionID}
	operationID, _ := identity.NewOperationID(rand.Reader)
	if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{
		{ID: "dev", Name: "Dev", GatewayURL: gateways["dev"].URL}, {ID: "prod", Name: "Prod", GatewayURL: gateways["prod"].URL},
	}, operationID, time.Now()); err != nil {
		t.Fatal(err)
	}
	classify, _ := identity.NewChangeClusterEnvironment(db, rand.Reader, time.Now)
	_, _ = classify.Execute(ctx, admin, "dev", "", authorization.EnvironmentNonProduction, "fixture")
	_, _ = classify.Execute(ctx, admin, "prod", "", authorization.EnvironmentProduction, "fixture")
	create, _ := identity.NewCreateLocalAccount(db, rand.Reader, time.Now)
	grants, _ := identity.NewChangeRoleGrant(db, rand.Reader, time.Now)
	users := map[string]session.Credentials{}
	for _, role := range []authorization.Role{authorization.RoleViewer, authorization.RoleOperator} {
		username := strings.ToLower(string(role))
		created, err := create.Execute(ctx, admin, username, username+" temporary passphrase")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grants.Execute(ctx, admin, created.Account.ID, "", authorization.RoleNone, role); err != nil {
			t.Fatal(err)
		}
		users[username] = signIn(username, username+" temporary passphrase")
	}

	users["admin"] = adminCredentials
	reason := ""
	send := func(user session.Credentials, method, path, body string) (int, string, *http.Response) {
		t.Helper()
		request, _ := http.NewRequest(method, origin+path, strings.NewReader(body))
		if reason != "" {
			request.Header.Set("X-Kafka3O-Change-Reason", reason)
		}
		request.Header.Set("Origin", origin)
		request.AddCookie(&http.Cookie{Name: session.CookieName, Value: user.Token})
		request.AddCookie(&http.Cookie{Name: session.CSRFCookieName, Value: user.CSRFToken})
		if method != http.MethodGet {
			request.Header.Set(session.CSRFHeader, user.CSRFToken)
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := browser.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(data), response
	}
	kafkaCalls := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(calls)
	}
	produce := `{"value": "exact  bytes"}`
	if status, body, _ := send(users["viewer"], "GET", "/api/clusters", ""); status != 200 || !strings.Contains(body, `"id":"prod"`) ||
		!strings.Contains(body, `"environment":"PRODUCTION"`) || strings.Contains(body, "127.0.0.1") {
		t.Fatalf("viewer clusters = %d %s", status, body)
	}
	for _, tt := range []struct {
		name, user, method, path, body string
		status                         int
		code                           string
	}{
		{"viewer reads prod", "viewer", "GET", "/api/clusters/prod/gateway/api/v1/topics", "", 200, ""},
		{"viewer cannot produce", "viewer", "POST", "/api/clusters/dev/gateway/api/v1/topics/orders/messages", produce, 403, "forbidden"},
		{"operator produces dev", "operator", "POST", "/api/clusters/dev/gateway/api/v1/topics/orders/messages", produce, 200, ""},
		{"operator cannot change prod", "operator", "DELETE", "/api/clusters/prod/gateway/api/v1/topics/orders", "", 403, "forbidden"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := kafkaCalls()
			status, body, _ := send(users[tt.user], tt.method, tt.path, tt.body)
			if status != tt.status || (tt.code != "" && !strings.Contains(body, `"code":"`+tt.code+`"`)) {
				t.Fatalf("status=%d body=%s", status, body)
			}
			want := before
			if tt.status == 200 {
				want++
			}
			if kafkaCalls() != want {
				t.Fatalf("Kafka calls = %d, want %d", kafkaCalls()-before, want-before)
			}
		})
	}
	// Production: ADMIN needs a reason, which is audited; OPERATOR stays denied even with one.
	before := kafkaCalls()
	if status, body, _ := send(users["admin"], "POST", "/api/clusters/prod/gateway/api/v1/topics/orders/messages", produce); status != 403 ||
		!strings.Contains(body, "change_reason_required") || kafkaCalls() != before {
		t.Fatalf("Production ADMIN write without a reason = %d %s", status, body)
	}
	reason = "Incident 42 hotfix"
	if status, body, _ := send(users["operator"], "POST", "/api/clusters/prod/gateway/api/v1/topics/orders/messages", produce); status != 403 || kafkaCalls() != before {
		t.Fatalf("Production OPERATOR write with a reason = %d %s", status, body)
	}
	if status, body, _ := send(users["admin"], "POST", "/api/clusters/prod/gateway/api/v1/topics/orders/messages", produce); status != 200 || kafkaCalls() != before+1 {
		t.Fatalf("Production ADMIN write with a reason = %d %s", status, body)
	}
	reason = ""
	events, err := db.ListAuditEvents(ctx, storage.AuditFilter{ClusterID: "prod", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	audited := false
	for _, event := range events {
		if event.ResourceKind == "gateway_request" && event.Decision == "allowed" && event.Reason == "Incident 42 hotfix" && event.ActorID == admin.AccountID {
			audited = true
		}
	}
	if !audited {
		t.Fatal("the Production change reason was not audited with the allowed decision")
	}
	// Each allowed mutation has a correlated outcome, permitted reads left no audit events, nothing is unresolved.
	all, err := db.ListAuditEvents(ctx, storage.AuditFilter{Global: true, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	intents, outcomes := map[string]bool{}, map[string]storage.AuditOutcome{}
	for _, event := range all {
		switch {
		case event.ResourceKind == "gateway_request" && event.Decision == "allowed":
			if !authorization.IsKafkaMutation(authorization.Action(event.Action)) {
				t.Fatalf("a permitted read was audited: %+v", event)
			}
			intents[event.OperationID] = true
		case event.ResourceKind == "gateway_outcome":
			outcomes[event.OperationID] = event.Outcome
		}
	}
	if len(intents) != 2 || len(outcomes) != 2 {
		t.Fatalf("intents=%v outcomes=%v", intents, outcomes)
	}
	for operationID := range intents {
		if outcomes[operationID] != storage.AuditConfirmed {
			t.Fatalf("intent %s outcome = %q", operationID, outcomes[operationID])
		}
	}
	if unresolved, err := db.ListUnresolvedGatewayIntents(ctx, time.Now().Add(time.Hour), 10); err != nil || len(unresolved) != 0 {
		t.Fatalf("unresolved after confirmed writes = %+v, %v", unresolved, err)
	}
	mu.Lock()
	last := calls[len(calls)-1]
	mu.Unlock()
	if last != (recordedKafkaCall{"prod", "POST", "/api/v1/topics/orders/messages", produce}) {
		t.Fatalf("the Gateway received %+v", last)
	}

	before = kafkaCalls()
	gateways["prod"].Close()
	if status, body, _ := send(users["viewer"], "GET", "/api/clusters/prod/gateway/api/v1/topics", ""); status != 503 || !strings.Contains(body, "gateway_unavailable") {
		t.Fatalf("stopped Gateway read = %d %s", status, body)
	}
	if status, body, _ := send(users["operator"], "POST", "/api/clusters/prod/gateway/api/v1/topics/orders/messages", produce); status != 503 || !strings.Contains(body, "gateway_outcome_unknown") {
		t.Fatalf("write to a stopped Gateway = %d %s", status, body)
	}
	policy.Close()
	if status, body, _ := send(users["operator"], "POST", "/api/clusters/dev/gateway/api/v1/topics/orders/messages", produce); status != 503 || !strings.Contains(body, "authorization_unavailable") {
		t.Fatalf("policy outage = %d %s", status, body)
	}
	if err := sessions.Revoke(ctx, users["viewer"].Token); err != nil {
		t.Fatal(err)
	}
	status, _, response := send(users["viewer"], "GET", "/api/clusters/dev/gateway/api/v1/topics", "")
	cleared := false
	for _, cookie := range response.Cookies() {
		if cookie.Name == session.CookieName && cookie.MaxAge < 0 {
			cleared = true
		}
	}
	if status != 401 || !cleared {
		t.Fatalf("revoked session = %d cleared=%t", status, cleared)
	}
	if kafkaCalls() != before {
		t.Fatalf("outages and revocation reached Kafka %d times", kafkaCalls()-before)
	}
}

// newSecuredGatewayServerForTest mirrors the secured Gateway listener: optional client certificates, Console only.
func newSecuredGatewayServerForTest(handler http.Handler, identity tls.Certificate, ca *servicetlstest.Authority) *http.Server {
	return &http.Server{Handler: handler, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity},
		ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: ca.Pool}}
}
