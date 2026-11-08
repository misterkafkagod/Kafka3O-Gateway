package gatewayproxy

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

// gatewayClient calls one cluster's Gateway with the Console identity and accepts only that Gateway's URI SAN.
// Redirects are never followed and requests are never retried.
func gatewayClient(clusterID string, identity tls.Certificate, serviceCA *x509.CertPool) *http.Client {
	want := servicetls.GatewayURIPrefix + clusterID
	return &http.Client{
		Timeout: Timeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, RootCAs: serviceCA, Certificates: []tls.Certificate{identity},
			VerifyPeerCertificate: servicetls.RequirePeerURI(func(uri string) bool { return uri == want }),
		}, MaxIdleConnsPerHost: 16},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
