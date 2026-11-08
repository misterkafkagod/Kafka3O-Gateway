package servicetls_test

import (
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestServiceIdentitiesAuthenticateTheGatewayCluster(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cluster, ok := servicetls.GatewayCluster(r.TLS)
		if !ok {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		io.WriteString(w, cluster)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{console.TLS},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.Pool}
	server.StartTLS()
	defer server.Close()
	call := func(identity servicetlstest.Identity) (int, string) {
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca.Pool,
			Certificates: []tls.Certificate{identity.TLS}, VerifyPeerCertificate: servicetls.RequirePeerURI(func(uri string) bool { return uri == servicetls.ConsoleURI })}}}
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}
	if status, cluster := call(ca.Issue(t, "gateway-prod", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))); status != 200 || cluster != "prod" {
		t.Fatalf("gateway identity = %d %q", status, cluster)
	}
	for _, uri := range []string{servicetls.ConsoleURI, servicetls.GatewayURIPrefix, servicetls.GatewayURIPrefix + "a/b", "spiffe://other/gateway/prod"} {
		if status, _ := call(ca.Issue(t, "other", uri, time.Now().Add(time.Hour))); status != http.StatusForbidden {
			t.Fatalf("%s accepted as a Gateway: %d", uri, status)
		}
	}
	other := servicetlstest.NewAuthority(t)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: other.Pool}}}
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("a server outside the service CA was trusted")
	}
	if servicetls.RequirePeerURI(func(string) bool { return true })(nil, nil) == nil {
		t.Fatal("an unverified peer passed the URI check")
	}
}

func TestLoadServiceIdentityChecksKeyCertificateAndURI(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	if cert, pool, err := servicetls.LoadServiceIdentity(console.CertificateFile, console.KeyFile, ca.CAFile, servicetls.ConsoleURI); err != nil || cert.Leaf == nil || pool == nil {
		t.Fatalf("load = %v", err)
	}
	if _, _, err := servicetls.LoadServiceIdentity(console.CertificateFile, console.KeyFile, ca.CAFile, servicetls.GatewayURIPrefix+"prod"); !errors.Is(err, servicetls.ErrInvalidServiceTLS) {
		t.Fatalf("wrong URI error = %v", err)
	}
	expired := ca.Issue(t, "expired", servicetls.ConsoleURI, time.Now().Add(-time.Second))
	if _, _, err := servicetls.LoadServiceIdentity(expired.CertificateFile, expired.KeyFile, ca.CAFile, servicetls.ConsoleURI); !errors.Is(err, servicetls.ErrInvalidServiceTLS) {
		t.Fatalf("expired error = %v", err)
	}
	if err := os.Chmod(console.KeyFile, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := servicetls.LoadServiceIdentity(console.CertificateFile, console.KeyFile, ca.CAFile, servicetls.ConsoleURI); !errors.Is(err, servicetls.ErrInvalidServiceTLS) {
		t.Fatalf("readable key error = %v", err)
	}
	if err := os.Chmod(console.KeyFile, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := servicetls.LoadServiceIdentity(console.CertificateFile, console.KeyFile, console.CertificateFile+".missing", servicetls.ConsoleURI); err == nil {
		t.Fatal("a missing CA file was accepted")
	}
}
