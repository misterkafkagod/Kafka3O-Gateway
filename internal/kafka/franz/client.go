// Package franz is the franz-go adapter for internal/kafka (TECH-SPEC §1.1):
// it translates between the port's domain types and the franz-go / kadm
// client libraries. It never carries policy, bounds, or audit (TECH-SPEC S4).
//
// The package imports internal/kafka, internal/telemetry, and franz-go only
// (TECH-SPEC §5.3) — not internal/config: the composition root maps the
// loaded Config onto the plain value types here (Config, TLSConfig, SASLConfig).
package franz

import (
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
)

// Config is everything the adapter needs to reach the cluster (FUNC-SPEC D1, X6).
type Config struct {
	Bootstrap      []string
	RequestTimeout time.Duration
	TLS            TLSConfig
	SASL           SASLConfig
}

// Client is the franz-go adapter. It holds one shared kgo.Client for admin
// and produce calls (TECH-SPEC §2.3 client lifecycle) plus its kadm wrapper.
type Client struct {
	kgo  *kgo.Client
	kadm *kadm.Client
}

// New builds a Client from Config: TLS (min 1.2), the selected SASL
// mechanism, and franz-go's kotel hooks for traces and metrics (TECH-SPEC
// §1.1, D5). The hooks read the global OpenTelemetry providers, so they work
// whether or not internal/telemetry (Task 1.9) has installed custom ones yet.
func New(c Config) (*Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Bootstrap...),
		kgo.WithHooks(kotelHooks()...),
		// Producer defaults for M5-M8 (TECH-SPEC C4): acks=all; idempotence
		// is kgo's own default and is never disabled here. explicitOrDefaultPartitioner
		// honours a ProduceRequest's explicit Partition when set (Task 5.1).
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(explicitOrDefaultPartitioner{}),
	}
	if c.RequestTimeout > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(c.RequestTimeout))
	}
	if opt, err := tlsOpt(c.TLS); err != nil {
		return nil, err
	} else if opt != nil {
		opts = append(opts, opt)
	}
	if opt, err := saslOpt(c.SASL); err != nil {
		return nil, err
	} else if opt != nil {
		opts = append(opts, opt)
	}

	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Client{kgo: kc, kadm: kadm.NewClient(kc)}, nil
}

// kotelHooks returns the franz-go hooks that emit traces and metrics through
// whichever global OpenTelemetry providers are installed (TECH-SPEC D5).
func kotelHooks() []kgo.Hook {
	kt := kotel.NewKotel(
		kotel.WithTracer(kotel.NewTracer()),
		kotel.WithMeter(kotel.NewMeter()),
	)
	return kt.Hooks()
}

// Close releases the underlying client (TECH-SPEC §5.4 shutdown order).
func (c *Client) Close() { c.kgo.Close() }
