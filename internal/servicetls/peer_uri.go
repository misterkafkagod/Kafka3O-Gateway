package servicetls

import "crypto/x509"

// peerURI returns the only URI SAN of a verified leaf certificate.
func peerURI(chains [][]*x509.Certificate) (string, bool) {
	if len(chains) == 0 || len(chains[0]) == 0 || len(chains[0][0].URIs) != 1 {
		return "", false
	}
	return chains[0][0].URIs[0].String(), true
}
