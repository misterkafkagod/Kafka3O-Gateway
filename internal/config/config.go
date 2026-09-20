// Package config holds the gateway's start-up configuration (TECH-SPEC §5.1).
//
// A Config is read once by Load, validated, and then treated as immutable:
// every switch, bound, key, and sink is fixed for the life of the process, and
// changing any of them means a restart (FUNC-SPEC §8.2, §9.6). Values come from
// an optional YAML file overridden by KGW_-prefixed environment variables.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config is the complete, validated configuration tree.
type Config struct {
	Kafka     Kafka     `koanf:"kafka"`
	HTTP      HTTP      `koanf:"http"`
	Auth      Auth      `koanf:"auth"`
	Policy    Policy    `koanf:"policy"`
	Bounds    Bounds    `koanf:"bounds"`
	Audit     Audit     `koanf:"audit"`
	Telemetry Telemetry `koanf:"telemetry"`
}

// Kafka describes the single managed cluster (FUNC-SPEC D1, X6).
type Kafka struct {
	Bootstrap      []string      `koanf:"bootstrap"`
	RequestTimeout time.Duration `koanf:"request_timeout"`
	TLS            KafkaTLS      `koanf:"tls"`
	SASL           SASL          `koanf:"sasl"`
	Producer       Producer      `koanf:"producer"`
	Consumer       Consumer      `koanf:"consumer"`
}

// KafkaTLS configures the broker connection's TLS; KeyFile is redacted from logs.
type KafkaTLS struct {
	Enabled  bool   `koanf:"enabled"`
	CAFile   string `koanf:"ca_file"`
	CertFile string `koanf:"cert_file"`
	KeyFile  string `koanf:"key_file"`
}

// SASL selects the authentication mechanism; Password is redacted from logs.
type SASL struct {
	Mechanism string `koanf:"mechanism"`
	Username  string `koanf:"username"`
	Password  string `koanf:"password"`
}

// Producer defaults for M5–M8 (TECH-SPEC C4).
type Producer struct {
	Acks       string `koanf:"acks"`
	Idempotent bool   `koanf:"idempotent"`
}

// Consumer defaults for M1–M4 and M8 (TECH-SPEC C4).
type Consumer struct {
	IsolationLevel string `koanf:"isolation_level"`
}

// HTTP configures the listener (TECH-SPEC C6, B6, C13).
type HTTP struct {
	Addr              string        `koanf:"addr"`
	TLS               ListenerTLS   `koanf:"tls"`
	ReadHeaderTimeout time.Duration `koanf:"read_header_timeout"`
	IdleTimeout       time.Duration `koanf:"idle_timeout"`
	// WriteTimeout is derived by Validate when zero: scan max-time ceiling + 5 s.
	WriteTimeout   time.Duration `koanf:"write_timeout"`
	BodyLimit      ByteSize      `koanf:"body_limit"`
	TrustedProxies []string      `koanf:"trusted_proxies"`
	CORS           CORS          `koanf:"cors"`
	Docs           Docs          `koanf:"docs"`
}

// ListenerTLS enables native TLS on the HTTP listener; KeyFile is redacted from logs.
type ListenerTLS struct {
	CertFile string `koanf:"cert_file"`
	KeyFile  string `koanf:"key_file"`
}

// CORS lists the browser origins allowed to call the gateway (FUNC-SPEC X5).
type CORS struct {
	Origins []string `koanf:"origins"`
}

// Docs toggles the Huma documentation UI at /docs.
type Docs struct {
	Enabled bool `koanf:"enabled"`
}

// Auth holds the API-key tiers (FUNC-SPEC D3, F1; TECH-SPEC B4).
type Auth struct {
	Enabled bool  `koanf:"enabled"`
	Keys    []Key `koanf:"keys"`
}

// Key is one configured API key: only its SHA-256 digest is ever stored.
type Key struct {
	ID     string `koanf:"id"`
	Tier   string `koanf:"tier"`
	SHA256 string `koanf:"sha256"`
}

// Tier values (FUNC-SPEC F1).
const (
	TierReader   = "reader"
	TierOperator = "operator"
)

// Policy holds the safety switches F2, F3, F6 (FUNC-SPEC §5.5).
type Policy struct {
	ReadOnlyMode       bool     `koanf:"read_only_mode"`
	DataPlaneLock      bool     `koanf:"data_plane_lock"`
	DisabledOperations []string `koanf:"disabled_operations"`
}

// Bounds are the per-request limits of FUNC-SPEC §8.8: every row has a default
// the caller gets when silent and a ceiling the caller cannot exceed (O3).
type Bounds struct {
	Read            ReadBounds       `koanf:"read"`
	Scan            ScanBounds       `koanf:"scan"`
	Replay          ReplayBounds     `koanf:"replay"`
	BulkProduceBody ByteSize         `koanf:"bulk_produce_body"`
	Throughput      ThroughputBounds `koanf:"throughput"`
	Page            PageBounds       `koanf:"page"`
}

// ReadBounds bound M1.
type ReadBounds struct {
	Limit Range[int] `koanf:"limit"`
}

// ScanBounds bound M3 and M4.
type ScanBounds struct {
	MaxScan      Range[int]           `koanf:"max_scan"`
	MaxMatches   Range[int]           `koanf:"max_matches"`
	MaxBytes     Range[ByteSize]      `koanf:"max_bytes"`
	MaxTime      Range[time.Duration] `koanf:"max_time"`
	RegexTimeout time.Duration        `koanf:"regex_timeout"`
}

// ReplayBounds bound M8 (FUNC-SPEC D4: N records per call).
type ReplayBounds struct {
	Limit Range[int] `koanf:"limit"`
}

// ThroughputBounds bound the C10 sample window.
type ThroughputBounds struct {
	Seconds Range[int] `koanf:"seconds"`
}

// PageBounds bound every paginated list (FUNC-SPEC §8.2).
type PageBounds struct {
	Size Range[int] `koanf:"size"`
}

// Range pairs a default with a hard ceiling for one bound.
type Range[T ~int | ~int64] struct {
	Default T `koanf:"default"`
	Ceiling T `koanf:"ceiling"`
}

// Audit selects the audit sinks (FUNC-SPEC §8.5; TECH-SPEC B5).
type Audit struct {
	// Sink is "stdout" (always on) or "kafka" (stdout plus the topic, fail-closed).
	Sink  string `koanf:"sink"`
	Topic string `koanf:"topic"`
}

// Audit sink names.
const (
	AuditSinkStdout = "stdout"
	AuditSinkKafka  = "kafka"
)

// Telemetry wires OpenTelemetry export (TECH-SPEC §1.0, §1.1).
type Telemetry struct {
	OTLP        OTLP   `koanf:"otlp"`
	ServiceName string `koanf:"service_name"`
	LogLevel    string `koanf:"log_level"`
}

// OTLP is the exporter target; an empty endpoint disables export.
type OTLP struct {
	Endpoint string `koanf:"endpoint"`
	Protocol string `koanf:"protocol"`
}

// ByteSize is a size in bytes that accepts "1MiB", "10MB", "512KiB", or a plain
// number in YAML and environment values.
type ByteSize int64

// Binary size units.
const (
	KiB ByteSize = 1 << 10
	MiB ByteSize = 1 << 20
	GiB ByteSize = 1 << 30
)

// UnmarshalText implements encoding.TextUnmarshaler for koanf's decode hook.
func (b *ByteSize) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	units := []struct {
		suffix string
		mult   ByteSize
	}{
		{"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB},
		{"GB", GiB}, {"MB", MiB}, {"KB", KiB},
		{"B", 1},
	}
	mult := ByteSize(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			mult = u.mult
			s = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("invalid byte size %q", string(text))
	}
	*b = ByteSize(n) * mult
	return nil
}

// MarshalText renders the size with its largest exact binary unit ("10MiB").
func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

// String renders the size with the largest exact binary unit.
func (b ByteSize) String() string {
	switch {
	case b >= GiB && b%GiB == 0:
		return strconv.FormatInt(int64(b/GiB), 10) + "GiB"
	case b >= MiB && b%MiB == 0:
		return strconv.FormatInt(int64(b/MiB), 10) + "MiB"
	case b >= KiB && b%KiB == 0:
		return strconv.FormatInt(int64(b/KiB), 10) + "KiB"
	default:
		return strconv.FormatInt(int64(b), 10) + "B"
	}
}

// Default returns the configuration with every value at its specified default.
// FUNC-SPEC §8.8 is the source for every bound; TECH-SPEC §6.3 C4/C6 for the rest.
func Default() Config {
	return Config{
		Kafka: Kafka{
			RequestTimeout: 30 * time.Second,
			Producer:       Producer{Acks: "all", Idempotent: true},
			Consumer:       Consumer{IsolationLevel: "read_committed"},
		},
		HTTP: HTTP{
			Addr:              ":8080",
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       60 * time.Second,
			BodyLimit:         1 * MiB,
			Docs:              Docs{Enabled: true},
		},
		Auth: Auth{Enabled: true},
		Bounds: Bounds{
			Read: ReadBounds{Limit: Range[int]{Default: 100, Ceiling: 1000}},
			Scan: ScanBounds{
				MaxScan:      Range[int]{Default: 10_000, Ceiling: 100_000},
				MaxMatches:   Range[int]{Default: 100, Ceiling: 1000},
				MaxBytes:     Range[ByteSize]{Default: 10 * MiB, Ceiling: 100 * MiB},
				MaxTime:      Range[time.Duration]{Default: 10 * time.Second, Ceiling: 60 * time.Second},
				RegexTimeout: 100 * time.Millisecond,
			},
			Replay:          ReplayBounds{Limit: Range[int]{Default: 1000, Ceiling: 10_000}},
			BulkProduceBody: 10 * MiB,
			Throughput:      ThroughputBounds{Seconds: Range[int]{Default: 5, Ceiling: 60}},
			Page:            PageBounds{Size: Range[int]{Default: 50, Ceiling: 500}},
		},
		Audit: Audit{Sink: AuditSinkStdout},
		Telemetry: Telemetry{
			OTLP:        OTLP{Protocol: "grpc"},
			ServiceName: "kafka3o-gateway",
			LogLevel:    "info",
		},
	}
}

// ErrInvalid wraps every validation failure so callers can distinguish
// configuration problems from I/O errors.
var ErrInvalid = errors.New("invalid configuration")
