package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewGatewayHTTPServer_RequiresTLSAndAuthentication(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	fixture.Close()
	token := strings.Repeat("a", 32)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/api/v1/topics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	gateway := newGatewayHTTPServer("", mux, []string{"https://ui.example"}, token, certificate)
	if gateway.TLSConfig == nil || gateway.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("gateway TLS minimum version is missing")
	}
	server := httptest.NewUnstartedServer(gateway.Handler)
	server.TLS = gateway.TLSConfig
	server.StartTLS()
	defer server.Close()
	client := server.Client()

	response, err := client.Get(server.URL + "/api/v1/health/live")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Errorf("anonymous liveness = %d", response.StatusCode)
	}
	response, err = client.Get(server.URL + "/api/v1/topics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Errorf("anonymous topics = %d", response.StatusCode)
	}
	preflight, err := http.NewRequest(http.MethodOptions, server.URL+"/api/v1/topics", nil)
	if err != nil {
		t.Fatal(err)
	}
	preflight.Header.Set("Origin", "https://ui.example")
	preflight.Header.Set("Access-Control-Request-Method", http.MethodPost)
	preflight.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
	response, err = client.Do(preflight)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent ||
		response.Header.Get("Access-Control-Allow-Origin") != "https://ui.example" ||
		!strings.Contains(strings.ToLower(response.Header.Get("Access-Control-Allow-Headers")), "authorization") {
		t.Errorf("preflight = %d, headers = %v", response.StatusCode, response.Header)
	}
	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/topics", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Origin", "https://ui.example")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Access-Control-Allow-Origin") != "https://ui.example" {
		t.Errorf("authorized topics = %d, headers = %v", response.StatusCode, response.Header)
	}
	response, err = http.Get("http://" + server.Listener.Addr().String() + "/api/v1/topics")
	if err == nil {
		response.Body.Close()
		if response.StatusCode < 400 {
			t.Errorf("plaintext request was accepted with status %d", response.StatusCode)
		}
	}
}
