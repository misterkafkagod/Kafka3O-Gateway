package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewMetricsHTTPServer_RequiresTLSAndTheMetricsToken(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	fixture.Close()
	token := strings.Repeat("m", 32)
	metricsServer := newMetricsHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("kafka3o_topics 1\n")) }), token, certificate)
	if metricsServer.TLSConfig == nil || metricsServer.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("metrics TLS minimum version is missing")
	}
	server := httptest.NewUnstartedServer(metricsServer.Handler)
	server.TLS = metricsServer.TLSConfig
	server.StartTLS()
	defer server.Close()

	for name, authorization := range map[string]string{"no token": "", "wrong token": "Bearer " + strings.Repeat("x", 32)} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/metrics", nil)
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s = %d", name, response.StatusCode)
		}
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("with token = %d", response.StatusCode)
	}
}
