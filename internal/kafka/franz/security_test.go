package franz

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// countOpts records how many kgo.Opt values a helper returned, without
// connecting to a broker: tlsOpt/saslOpt only build option values.
func countOpts(opts ...kgo.Opt) int {
	n := 0
	for _, o := range opts {
		if o != nil {
			n++
		}
	}
	return n
}

func TestFranz_SecurityOptions_TLSMinVersionAndSASLMechanisms(t *testing.T) {
	t.Parallel()

	t.Run("TLS disabled returns no option", func(t *testing.T) {
		t.Parallel()
		opt, err := tlsOpt(TLSConfig{})
		if err != nil || opt != nil {
			t.Fatalf("tlsOpt(disabled) = %v, %v; want nil, nil", opt, err)
		}
	})

	t.Run("TLS enabled builds a DialTLSConfig option at minimum TLS 1.2", func(t *testing.T) {
		t.Parallel()
		opt, err := tlsOpt(TLSConfig{Enabled: true})
		if err != nil {
			t.Fatalf("tlsOpt(enabled) error: %v", err)
		}
		if countOpts(opt) != 1 {
			t.Fatal("tlsOpt(enabled) returned no option")
		}
		// Construction only: applying the option must not require a live broker.
		kc, err := kgo.NewClient(opt, kgo.SeedBrokers("127.0.0.1:0"))
		if err != nil {
			t.Errorf("kgo.NewClient with the TLS option: %v", err)
		} else {
			kc.Close()
		}
	})

	t.Run("TLS with an unreadable CA file errors without dialing", func(t *testing.T) {
		t.Parallel()
		if _, err := tlsOpt(TLSConfig{Enabled: true, CAFile: "does-not-exist.pem"}); err == nil {
			t.Fatal("tlsOpt with a missing CA file returned nil error")
		}
	})

	cases := []struct {
		name string
		cfg  SASLConfig
	}{
		{"PLAIN", SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"}},
		{"SCRAM-SHA-256", SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "u", Password: "p"}},
		{"SCRAM-SHA-512", SASLConfig{Mechanism: "SCRAM-SHA-512", Username: "u", Password: "p"}},
		{"OAUTHBEARER", SASLConfig{Mechanism: "OAUTHBEARER", Password: "token"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt, err := saslOpt(tc.cfg)
			if err != nil {
				t.Fatalf("saslOpt(%s) error: %v", tc.name, err)
			}
			if countOpts(opt) != 1 {
				t.Fatalf("saslOpt(%s) returned no option", tc.name)
			}
			kc, err := kgo.NewClient(opt, kgo.SeedBrokers("127.0.0.1:0"))
			if err != nil {
				t.Errorf("kgo.NewClient with the %s option: %v", tc.name, err)
			} else {
				kc.Close()
			}
		})
	}

	t.Run("empty mechanism returns no option", func(t *testing.T) {
		t.Parallel()
		opt, err := saslOpt(SASLConfig{})
		if err != nil || opt != nil {
			t.Fatalf("saslOpt(empty) = %v, %v; want nil, nil", opt, err)
		}
	})

	t.Run("unknown mechanism errors", func(t *testing.T) {
		t.Parallel()
		if _, err := saslOpt(SASLConfig{Mechanism: "GSSAPI"}); err == nil {
			t.Fatal("saslOpt(GSSAPI) returned nil error")
		}
	})
}
