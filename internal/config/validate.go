package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strings"
	"time"
)

// writeTimeoutSlack is added to the scan max-time ceiling to derive
// HTTP.WriteTimeout (TECH-SPEC C6): the longest bounded request plus headroom.
const writeTimeoutSlack = 5 * time.Second

var (
	digestRE      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	operationIDRE = regexp.MustCompile(`^[A-Z][0-9]{1,2}$`)
)

// Validate checks every rule of TECH-SPEC §6.1 B4/B6 and §6.3 C6/C13 and the
// FUNC-SPEC §8.8 bound invariants, reporting all failures at once. It also
// derives HTTP.WriteTimeout when unset and normalises key digests to lower case.
func Validate(c *Config) error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	validateKafka(&c.Kafka, add)
	validateHTTP(&c.HTTP, c.Auth.Enabled, c.Bounds.Scan.MaxTime.Ceiling, add)
	validateAuth(&c.Auth, add)
	validatePolicy(&c.Policy, add)
	validateBounds(&c.Bounds, add)
	validateAudit(&c.Audit, add)
	validateTelemetry(&c.Telemetry, add)

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w:\n  %w", ErrInvalid, errors.Join(errs...))
}

type adder func(format string, a ...any)

func validateKafka(k *Kafka, add adder) {
	if len(k.Bootstrap) == 0 {
		add("kafka.bootstrap: at least one host:port is required")
	}
	for _, b := range k.Bootstrap {
		if _, _, err := net.SplitHostPort(b); err != nil {
			add("kafka.bootstrap: %q is not host:port", b)
		}
	}
	if k.RequestTimeout <= 0 {
		add("kafka.request_timeout: must be > 0")
	}
	if k.TLS.Enabled && (k.TLS.CertFile != "") != (k.TLS.KeyFile != "") {
		add("kafka.tls: cert_file and key_file must be set together")
	}
	switch k.SASL.Mechanism {
	case "":
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if k.SASL.Username == "" || k.SASL.Password == "" {
			add("kafka.sasl: username and password are required for %s", k.SASL.Mechanism)
		}
	case "OAUTHBEARER":
	default:
		add("kafka.sasl.mechanism: %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER", k.SASL.Mechanism)
	}
	switch k.Producer.Acks {
	case "all", "1", "0":
	default:
		add("kafka.producer.acks: %q is not one of all, 1, 0", k.Producer.Acks)
	}
	switch k.Consumer.IsolationLevel {
	case "read_committed", "read_uncommitted":
	default:
		add("kafka.consumer.isolation_level: %q is not read_committed or read_uncommitted", k.Consumer.IsolationLevel)
	}
}

func validateHTTP(h *HTTP, authEnabled bool, maxTimeCeiling time.Duration, add adder) {
	if h.Addr == "" {
		add("http.addr: must not be empty")
	}
	if (h.TLS.CertFile != "") != (h.TLS.KeyFile != "") {
		add("http.tls: cert_file and key_file must be set together")
	}
	if h.ReadHeaderTimeout <= 0 {
		add("http.read_header_timeout: must be > 0")
	}
	if h.IdleTimeout <= 0 {
		add("http.idle_timeout: must be > 0")
	}
	if h.BodyLimit <= 0 {
		add("http.body_limit: must be > 0")
	}
	derived := maxTimeCeiling + writeTimeoutSlack
	switch {
	case h.WriteTimeout == 0:
		h.WriteTimeout = derived
	case h.WriteTimeout < derived:
		add("http.write_timeout: %s is below the derived minimum %s (scan max_time ceiling + %s)",
			h.WriteTimeout, derived, writeTimeoutSlack)
	}
	for _, p := range h.TrustedProxies {
		if _, err := netip.ParsePrefix(p); err != nil {
			add("http.trusted_proxies: %q is not a CIDR", p)
		}
	}
	for _, o := range h.CORS.Origins {
		if o == "*" && authEnabled {
			add("http.cors.origins: wildcard is not allowed while auth is enabled")
		}
	}
}

func validateAuth(a *Auth, add adder) {
	if a.Enabled && len(a.Keys) == 0 {
		add("auth.keys: at least one key is required while auth is enabled")
	}
	seen := map[string]bool{}
	for i := range a.Keys {
		k := &a.Keys[i]
		if k.ID == "" {
			add("auth.keys[%d].id: must not be empty", i)
		} else if seen[k.ID] {
			add("auth.keys[%d].id: duplicate id %q", i, k.ID)
		}
		seen[k.ID] = true
		if k.Tier != TierReader && k.Tier != TierOperator {
			add("auth.keys[%d].tier: %q is not reader or operator", i, k.Tier)
		}
		k.SHA256 = strings.ToLower(strings.TrimSpace(k.SHA256))
		if !digestRE.MatchString(k.SHA256) {
			add("auth.keys[%d].sha256: must be 64 hex characters", i)
		}
	}
}

// validatePolicy checks the shape of operation ids only; whether an id names a
// destructive command is decided by the command table (Task 1.3, gates 1.7).
func validatePolicy(p *Policy, add adder) {
	seen := map[string]bool{}
	for _, id := range p.DisabledOperations {
		if !operationIDRE.MatchString(id) {
			add("policy.disabled_operations: %q is not a command id", id)
		}
		if seen[id] {
			add("policy.disabled_operations: duplicate %q", id)
		}
		seen[id] = true
	}
}

func validateBounds(b *Bounds, add adder) {
	checkRange(add, "bounds.read.limit", b.Read.Limit)
	checkRange(add, "bounds.scan.max_scan", b.Scan.MaxScan)
	checkRange(add, "bounds.scan.max_matches", b.Scan.MaxMatches)
	checkRange(add, "bounds.scan.max_bytes", b.Scan.MaxBytes)
	checkRange(add, "bounds.scan.max_time", b.Scan.MaxTime)
	if b.Scan.RegexTimeout <= 0 {
		add("bounds.scan.regex_timeout: must be > 0")
	}
	checkRange(add, "bounds.replay.limit", b.Replay.Limit)
	if b.BulkProduceBody <= 0 {
		add("bounds.bulk_produce_body: must be > 0")
	}
	checkRange(add, "bounds.throughput.seconds", b.Throughput.Seconds)
	checkRange(add, "bounds.page.size", b.Page.Size)
}

func checkRange[T ~int | ~int64](add adder, path string, r Range[T]) {
	if r.Default <= 0 {
		add("%s.default: must be > 0", path)
	}
	if r.Ceiling < r.Default {
		add("%s.ceiling: %v is below the default %v", path, r.Ceiling, r.Default)
	}
}

func validateAudit(a *Audit, add adder) {
	switch a.Sink {
	case AuditSinkStdout:
	case AuditSinkKafka:
		if a.Topic == "" {
			add("audit.topic: required when audit.sink is kafka")
		}
	default:
		add("audit.sink: %q is not stdout or kafka", a.Sink)
	}
}

func validateTelemetry(t *Telemetry, add adder) {
	switch t.OTLP.Protocol {
	case "grpc", "http":
	default:
		add("telemetry.otlp.protocol: %q is not grpc or http", t.OTLP.Protocol)
	}
	switch t.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("telemetry.log_level: %q is not debug, info, warn, or error", t.LogLevel)
	}
	if t.ServiceName == "" {
		add("telemetry.service_name: must not be empty")
	}
}
