package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestSecuredGatewayServerAcceptsOnlyTheConsoleCertificate(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	serverIdentity := ca.Issue(t, "gateway-prod", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))
	server := newSecuredGatewayHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}), serverIdentity.TLS, ca.Pool)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.ServeTLS(listener, "", "")
	defer server.Close()
	url := "https://" + listener.Addr().String() + "/api/v1/health/live"
	call := func(identity *servicetlstest.Identity) error {
		config := &tls.Config{RootCAs: ca.Pool}
		if identity != nil {
			config.Certificates = []tls.Certificate{identity.TLS}
		}
		response, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: config}}).Get(url)
		if err == nil {
			response.Body.Close()
		}
		return err
	}
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	if err := call(nil); err != nil {
		t.Fatalf("anonymous probe failed: %v", err)
	}
	if err := call(&console); err != nil {
		t.Fatalf("Console handshake failed: %v", err)
	}
	other := ca.Issue(t, "gateway-dev", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	if err := call(&other); err == nil {
		t.Fatal("another service certificate completed the handshake")
	}
	if server.TLSConfig.MinVersion != tls.VersionTLS13 {
		t.Fatal("secured mode allows TLS below 1.3")
	}
}
