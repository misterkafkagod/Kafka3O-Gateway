package main

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
)

func newGatewayHTTPServer(addr string, handler http.Handler, origins []string, token string, certificate tls.Certificate) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           httpapi.WithCORS(httpapi.WithBearerAuth(handler, token), origins),
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		},
	}
}
