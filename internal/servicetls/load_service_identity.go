package servicetls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"runtime"
	"time"
)

// LoadServiceIdentity loads an owner-only key, its currently valid certificate (which must carry exactly ownURI)
// and the service CA pool that peers must chain to.
func LoadServiceIdentity(certificateFile, keyFile, caFile, ownURI string) (tls.Certificate, *x509.CertPool, error) {
	info, err := os.Stat(keyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("inspect service TLS key: %w", err)
	}
	if !info.Mode().IsRegular() || (runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0) {
		return tls.Certificate{}, nil, fmt.Errorf("%w: the service key must be an owner-only regular file", ErrInvalidServiceTLS)
	}
	certificate, err := tls.LoadX509KeyPair(certificateFile, keyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load service certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("parse service certificate: %w", err)
	}
	now := time.Now()
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != ownURI || now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return tls.Certificate{}, nil, fmt.Errorf("%w: the service certificate must be valid and carry exactly %s", ErrInvalidServiceTLS, ownURI)
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read service CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return tls.Certificate{}, nil, fmt.Errorf("%w: the service CA file has no certificates", ErrInvalidServiceTLS)
	}
	certificate.Leaf = leaf
	return certificate, pool, nil
}
