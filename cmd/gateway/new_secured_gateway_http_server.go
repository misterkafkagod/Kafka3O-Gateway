package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

// newSecuredGatewayHTTPServer serves only TLS 1.3 without CORS. A client certificate is optional so anonymous
// liveness probes work, but any certificate offered must be the Console's; the middleware requires it elsewhere.
func newSecuredGatewayHTTPServer(addr string, handler http.Handler, certificate tls.Certificate, serviceCA *x509.CertPool) *http.Server {
	requireConsole := servicetls.RequirePeerURI(func(uri string) bool { return uri == servicetls.ConsoleURI })
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{certificate},
			ClientAuth:   tls.VerifyClientCertIfGiven,
			ClientCAs:    serviceCA,
			VerifyPeerCertificate: func(raw [][]byte, chains [][]*x509.Certificate) error {
				if len(raw) == 0 {
					return nil
				}
				if len(chains) == 0 {
					return errors.New("unverified service certificate")
				}
				return requireConsole(raw, chains)
			},
		},
	}
}
