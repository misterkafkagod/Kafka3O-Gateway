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
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
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

type securedConsole struct {
	db       *storage.Database
	sessions *session.Manager
	admin    identity.LocalAdminActor
	mint     delegation.MintDelegation
}

// newSecuredConsole bootstraps a real Console store with dev (non-Production), prod (Production) and new
// (unclassified) clusters and returns a local session per requested global role.
func newSecuredConsole(t *testing.T) (securedConsole, map[string]string) {
	t.Helper()
	ctx := context.Background()
	client := netip.MustParseAddr("192.0.2.10")
	db, err := storage.OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{5}, 32))
	bootstrap, _ := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	authenticate, _ := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	change, _ := identity.NewChangeLocalPassword(authenticate, db, rand.Reader, time.Now)
	sessions, _ := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	signIn := func(username, temporary string) (identity.LocalAccount, session.Credentials) {
		t.Helper()
		account, err := authenticate.Execute(ctx, username, temporary, client)
		if err != nil {
			t.Fatal(err)
		}
		if err := change.Execute(ctx, account.ID, username, temporary, "replacement passphrase", client); err != nil {
			t.Fatal(err)
		}
		account, err = authenticate.Execute(ctx, username, "replacement passphrase", client)
		if err != nil {
			t.Fatal(err)
		}
		credentials, err := sessions.IssueLocal(ctx, account)
		if err != nil {
			t.Fatal(err)
		}
		return account, credentials
	}
	if _, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase"); err != nil {
		t.Fatal(err)
	}
	adminAccount, adminSession := signIn("admin", "initial passphrase")
	admin := identity.LocalAdminActor{AccountID: adminAccount.ID, SessionID: adminSession.Principal.SessionID}
	operationID, _ := identity.NewOperationID(rand.Reader)
	if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{
		{ID: "dev", Name: "Dev", GatewayURL: "https://dev.invalid"}, {ID: "prod", Name: "Prod", GatewayURL: "https://prod.invalid"},
		{ID: "new", Name: "New", GatewayURL: "https://new.invalid"},
	}, operationID, time.Now()); err != nil {
		t.Fatal(err)
	}
	classify, _ := identity.NewChangeClusterEnvironment(db, rand.Reader, time.Now)
	for cluster, environment := range map[string]authorization.Environment{"dev": authorization.EnvironmentNonProduction, "prod": authorization.EnvironmentProduction} {
		if _, err := classify.Execute(ctx, admin, cluster, "", environment, "integration fixture"); err != nil {
			t.Fatal(err)
		}
	}
	create, _ := identity.NewCreateLocalAccount(db, rand.Reader, time.Now)
	grants, _ := identity.NewChangeRoleGrant(db, rand.Reader, time.Now)
	sessionsByRole := map[string]string{}
	for _, role := range []authorization.Role{authorization.RoleViewer, authorization.RoleOperator} {
		username := strings.ToLower(string(role))
		created, err := create.Execute(ctx, admin, username, username+" temporary passphrase")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grants.Execute(ctx, admin, created.Account.ID, "", authorization.RoleNone, role); err != nil {
			t.Fatal(err)
		}
		_, credentials := signIn(username, username+" temporary passphrase")
		sessionsByRole[username] = credentials.Principal.SessionID
		if role == authorization.RoleOperator {
			sessionsByRole["operator-token"] = credentials.Token
		}
	}
	mint, _ := delegation.NewMintDelegation(db, rand.Reader, time.Now)
	return securedConsole{db: db, sessions: sessions, admin: admin, mint: mint}, sessionsByRole
}

func TestSecuredModeEndToEndEnforcementBeforeKafka(t *testing.T) {
	ctx := context.Background()
	console, sessionIDs := newSecuredConsole(t)
	ca := servicetlstest.NewAuthority(t)
	consoleIdentity := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	consume, err := delegation.NewConsumeDelegation(console.db, console.db, console.db, nil, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := delegation.NewRecordOutcome(console.db, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	policyHandler, err := policyapi.NewHandler(consume, outcome, console.db, logger)
	if err != nil {
		t.Fatal(err)
	}
	policyServer := httptest.NewUnstartedServer(policyHandler)
	policyServer.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{consoleIdentity.TLS},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
	policyServer.StartTLS()
	defer policyServer.Close()

	var kafkaCalls atomic.Int64
	gateways := map[string]string{}
	for _, cluster := range []string{"dev", "prod", "new"} {
		identity := ca.Issue(t, "gateway-"+cluster, servicetls.GatewayURIPrefix+cluster, time.Now().Add(time.Hour))
		policy, err := gatewayauth.NewConsolePolicyClient(policyServer.URL, identity.TLS, ca.Pool)
		if err != nil {
			t.Fatal(err)
		}
		kafka := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			kafkaCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		})
		handler, err := gatewayauth.WithDelegatedAuthorization(kafka, policy, cluster, logger)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(handler)
		server.Config = newSecuredGatewayHTTPServer("", handler, identity.TLS, ca.Pool)
		server.TLS = server.Config.TLSConfig
		server.StartTLS()
		defer server.Close()
		gateways[cluster] = server.URL
	}
	consoleClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool,
		Certificates: []tls.Certificate{consoleIdentity.TLS}}}}
	send := func(client *http.Client, cluster, method, path, body, handle string) int {
		t.Helper()
		request, _ := http.NewRequest(method, gateways[cluster]+path, strings.NewReader(body))
		if handle != "" {
			request.Header.Set(gatewayauth.DelegationHeader, handle)
		}
		response, err := client.Do(request)
		if err != nil {
			return -1
		}
		response.Body.Close()
		return response.StatusCode
	}
	mint := func(sessionID, cluster, method, path, body string, action authorization.Action) string {
		t.Helper()
		parsed, _ := url.Parse(path)
		binding := authorization.RequestBinding(cluster, method, parsed.EscapedPath(), parsed.Query(), []byte(body))
		handle, err := console.mint.Execute(ctx, sessionID, cluster, action, parsed.EscapedPath(), binding, "")
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	produce := `{"value":"synthetic"}`
	messages := "/api/v1/topics/orders/messages"

	// Matrix: roles across Production, non-Production and unclassified clusters.
	for _, tt := range []struct {
		name, role, cluster, method, path, body string
		action                                  authorization.Action
		status                                  int
	}{
		{"viewer reads prod", "viewer", "prod", "GET", "/api/v1/topics", "", authorization.ActionListTopics, 200},
		{"viewer produce dev", "viewer", "dev", "POST", messages, produce, authorization.ActionProduceMessage, 403},
		{"operator produce dev", "operator", "dev", "POST", messages, produce, authorization.ActionProduceMessage, 200},
		{"operator produce prod", "operator", "prod", "POST", messages, produce, authorization.ActionProduceMessage, 403},
		{"operator delete prod", "operator", "prod", "DELETE", "/api/v1/topics/orders", "", authorization.ActionDeleteTopic, 403},
		{"operator produce unclassified", "operator", "new", "POST", messages, produce, authorization.ActionProduceMessage, 403},
		{"operator alters prod config", "operator", "prod", "POST", "/api/v1/topics/orders/config", `{"name":"retention.ms","value":"1"}`, authorization.ActionAlterTopicConfig, 403},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := kafkaCalls.Load()
			handle := mint(sessionIDs[tt.role], tt.cluster, tt.method, tt.path, tt.body, tt.action)
			if status := send(consoleClient, tt.cluster, tt.method, tt.path, tt.body, handle); status != tt.status {
				t.Fatalf("status = %d, want %d", status, tt.status)
			}
			want := before
			if tt.status == 200 {
				want++
			}
			if kafkaCalls.Load() != want {
				t.Fatalf("Kafka calls = %d, want %d", kafkaCalls.Load()-before, want-before)
			}
		})
	}

	// Bypass attempts never reach Kafka.
	before := kafkaCalls.Load()
	operator := sessionIDs["operator"]
	valid := mint(operator, "dev", "POST", messages, produce, authorization.ActionProduceMessage)
	noCertificate := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool}}}
	other := ca.Issue(t, "intruder", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	otherService := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{other.TLS}}}}
	for _, tt := range []struct {
		name    string
		client  *http.Client
		cluster string
		method  string
		path    string
		body    string
		handle  string
		status  int
	}{
		{"direct call without client certificate", noCertificate, "dev", "POST", messages, produce, valid, 401},
		{"another service certificate", otherService, "dev", "POST", messages, produce, valid, -1},
		{"no delegation", consoleClient, "dev", "POST", messages, produce, "", 401},
		{"forged delegation", consoleClient, "dev", "POST", messages, produce, strings.Repeat("A", 43), 401},
		{"changed body", consoleClient, "dev", "POST", messages, `{"value":"evil"}`, mint(operator, "dev", "POST", messages, produce, authorization.ActionProduceMessage), 401},
		{"changed path", consoleClient, "dev", "POST", "/api/v1/topics/payments/messages", produce, mint(operator, "dev", "POST", messages, produce, authorization.ActionProduceMessage), 401},
		{"changed query", consoleClient, "dev", "GET", "/api/v1/topics/orders/messages?limit=1000", "", mint(operator, "dev", "GET", "/api/v1/topics/orders/messages?limit=10", "", authorization.ActionReadMessages), 401},
		{"wrong cluster", consoleClient, "prod", "POST", messages, produce, mint(operator, "dev", "POST", messages, produce, authorization.ActionProduceMessage), 401},
		{"unknown route", consoleClient, "dev", "GET", "/openapi.json", "", valid, 403},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if status := send(tt.client, tt.cluster, tt.method, tt.path, tt.body, tt.handle); status != tt.status {
				t.Fatalf("status = %d, want %d", status, tt.status)
			}
		})
	}
	if kafkaCalls.Load() != before {
		t.Fatalf("bypass attempts reached Kafka %d times", kafkaCalls.Load()-before)
	}
	if status := send(consoleClient, "dev", "POST", messages, produce, valid); status != 200 {
		t.Fatalf("the valid delegation failed after unrelated rejections: %d", status)
	}
	if status := send(consoleClient, "dev", "POST", messages, produce, valid); status != 401 {
		t.Fatalf("replayed delegation status = %d", status)
	}

	// Revocation applies to the next decision; liveness stays anonymous; a policy outage fails closed.
	revoked := mint(operator, "dev", "GET", "/api/v1/topics", "", authorization.ActionListTopics)
	if err := console.sessions.Revoke(ctx, sessionIDs["operator-token"]); err != nil {
		t.Fatal(err)
	}
	before = kafkaCalls.Load()
	if status := send(consoleClient, "dev", "GET", "/api/v1/topics", "", revoked); status != 401 {
		t.Fatalf("revoked session status = %d", status)
	}
	if status := send(noCertificate, "dev", "GET", "/api/v1/health/live", "", ""); status != 200 {
		t.Fatalf("anonymous liveness status = %d", status)
	}
	outage := mint(sessionIDs["viewer"], "dev", "GET", "/api/v1/topics", "", authorization.ActionListTopics)
	policyServer.Close()
	if status := send(consoleClient, "dev", "GET", "/api/v1/topics", "", outage); status != 503 {
		t.Fatalf("policy outage status = %d", status)
	}
	if kafkaCalls.Load() != before+1 {
		t.Fatalf("Kafka calls after revocation and outage = %d, want only the liveness probe", kafkaCalls.Load()-before)
	}
	events, err := console.db.ListAuditEvents(ctx, storage.AuditFilter{ClusterID: "prod", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	denials := map[string]int{}
	for _, event := range events {
		if event.ResourceKind == "gateway_request" && strings.HasPrefix(event.Decision, "denied:") {
			denials[event.Decision]++
		}
	}
	// Three OPERATOR mutations on prod, plus the dev delegation presented to the prod Gateway.
	if denials["denied:production_admin_required"] != 3 || denials["denied:invalid_delegation"] != 1 || len(denials) != 2 {
		t.Fatalf("audited Production denials = %v", denials)
	}
}
