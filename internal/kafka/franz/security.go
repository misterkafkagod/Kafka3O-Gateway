package franz

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// TLSConfig configures the broker connection's TLS (FUNC-SPEC X6).
// It is a plain value type, not internal/config.KafkaTLS: internal/kafka/franz
// may not import internal/config (TECH-SPEC §5.3), so the composition root
// (internal/app, Task 1.10) maps the loaded Config onto this type.
type TLSConfig struct {
	Enabled  bool
	CAFile   string
	CertFile string
	KeyFile  string
}

// SASLConfig selects the SASL mechanism (FUNC-SPEC X6). An empty Mechanism
// means PLAINTEXT, or mTLS-only when TLSConfig.Enabled is set.
type SASLConfig struct {
	Mechanism string // "", PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
	Username  string
	Password  string
}

// loadCAPool reads a PEM-encoded CA bundle from path.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // G304: path is operator-supplied start-up config (TECH-SPEC §5.4), not request input
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no PEM certificates found in %s", path)
	}
	return pool, nil
}

// tlsOpt builds the kgo.Opt for the broker connection's TLS, minimum
// version 1.2 (FUNC-SPEC X6), or nil when TLS is disabled.
func tlsOpt(c TLSConfig) (kgo.Opt, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pool, err := loadCAPool(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("franz: TLS CA file: %w", err)
		}
		cfg.RootCAs = pool
	}
	if c.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("franz: TLS client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return kgo.DialTLSConfig(cfg), nil
}

// saslOpt builds the kgo.Opt selecting the SASL mechanism (FUNC-SPEC X6):
// PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, or OAUTHBEARER. An empty mechanism
// returns nil.
func saslOpt(c SASLConfig) (kgo.Opt, error) {
	var mech sasl.Mechanism
	switch c.Mechanism {
	case "":
		return nil, nil
	case "PLAIN":
		mech = plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism()
	case "SCRAM-SHA-256":
		mech = scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		mech = scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism()
	case "OAUTHBEARER":
		mech = oauth.Auth{Token: c.Password}.AsMechanism()
	default:
		// internal/config.Validate already rejects unknown mechanisms; defence in depth.
		return nil, fmt.Errorf("franz: SASL mechanism: unknown %q", c.Mechanism)
	}
	return kgo.SASL(mech), nil
}
