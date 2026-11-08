package servicetls

import (
	"crypto/x509"
	"errors"
)

// RequirePeerURI is a tls.Config VerifyPeerCertificate hook accepting only a verified chain whose leaf's single
// URI SAN satisfies accept.
func RequirePeerURI(accept func(string) bool) func([][]byte, [][]*x509.Certificate) error {
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		uri, ok := peerURI(chains)
		if !ok || !accept(uri) {
			return errors.New("unexpected service identity")
		}
		return nil
	}
}
