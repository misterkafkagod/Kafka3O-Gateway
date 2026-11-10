package main

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
)

// newMetricsHTTPServer is the optional Prometheus listener: TLS with the Gateway certificate and its own bearer token.
func newMetricsHTTPServer(addr string, handler http.Handler, token string, certificate tls.Certificate) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           httpapi.WithBearerAuth(handler, token),
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		},
	}
}
