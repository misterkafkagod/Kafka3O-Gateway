package config

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	digestA = "0000000000000000000000000000000000000000000000000000000000000001"
	digestB = "0000000000000000000000000000000000000000000000000000000000000002"
)

// valid returns a minimal configuration that passes Validate; tests mutate it.
func valid() Config {
	c := Default()
	c.Kafka.Bootstrap = []string{"localhost:9092"}
	c.Auth.Keys = []Key{{ID: "ops", Tier: TierOperator, SHA256: digestA}}
	return c
}

func mustValidate(t *testing.T, c *Config) {
	t.Helper()
	if err := Validate(c); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
}

// expectInvalid asserts Validate fails and that the message names the offending key.
func expectInvalid(t *testing.T, c *Config, wantSubstr string) {
	t.Helper()
	err := Validate(c)
	if err == nil {
		t.Fatalf("Validate() = nil, want error mentioning %q", wantSubstr)
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("Validate() error does not wrap ErrInvalid: %v", err)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("Validate() error = %q, want it to mention %q", err, wantSubstr)
	}
}

func TestLoad_YAMLThenEnvOverrides(t *testing.T) {
	t.Parallel()
	env := []string{
		// Derived names (TECH-SPEC §5.4) — scalar, duration, bytes, bool, list.
		"KGW_KAFKA_BOOTSTRAP=env-1:9092, env-2:9092",
		"KGW_HTTP_READ_HEADER_TIMEOUT=3s",
		"KGW_HTTP_BODY_LIMIT=3MiB",
		"KGW_HTTP_TLS_CERT=/env/http.pem",
		"KGW_HTTP_TLS_KEY=/env/http-key.pem",
		"KGW_POLICY_READ_ONLY_MODE=false",
		"KGW_HTTP_TRUSTED_PROXIES=192.168.0.0/16,172.16.0.0/12",
		"KGW_BOUNDS_SCAN_MAX_MATCHES_CEILING=333",
		// Spec-named aliases (TECH-SPEC §6.3).
		`KGW_API_KEYS=[{"id":"env-key","tier":"reader","sha256":"` + digestB + `"}]`,
		"KGW_CORS_ORIGINS=https://a.example,https://b.example",
		"KGW_DOCS_ENABLED=true",
		"KGW_KAFKA_ISOLATION_LEVEL=read_committed",
		"KGW_AUDIT_TOPIC=env-audit",
		// Reserved variables are ignored, and non-KGW variables never matter.
		"KGW_PROBE_API_KEY=whatever",
		"KGW_CONFIG=/ignored/by/load",
		"PATH=/usr/bin",
	}
	cfg, err := Load(Options{Path: filepath.Join("testdata", "full.yaml"), Environ: env})
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	got := map[string]any{
		"kafka.bootstrap":                cfg.Kafka.Bootstrap,
		"http.read_header_timeout":       cfg.HTTP.ReadHeaderTimeout,
		"http.body_limit":                cfg.HTTP.BodyLimit,
		"http.tls":                       cfg.HTTP.TLS,
		"policy.read_only_mode":          cfg.Policy.ReadOnlyMode,
		"http.trusted_proxies":           cfg.HTTP.TrustedProxies,
		"bounds.scan.max_matches":        cfg.Bounds.Scan.MaxMatches,
		"auth.keys":                      cfg.Auth.Keys,
		"http.cors.origins":              cfg.HTTP.CORS.Origins,
		"http.docs.enabled":              cfg.HTTP.Docs.Enabled,
		"kafka.consumer.isolation_level": cfg.Kafka.Consumer.IsolationLevel,
		"audit.topic":                    cfg.Audit.Topic,
		// YAML values the environment did not touch survive.
		"kafka.sasl.mechanism": cfg.Kafka.SASL.Mechanism,
		"http.addr":            cfg.HTTP.Addr,
		"bounds.page.size":     cfg.Bounds.Page.Size,
		"telemetry.log_level":  cfg.Telemetry.LogLevel,
		// Defaults neither layer touched survive too.
		"kafka.producer.acks": cfg.Kafka.Producer.Acks,
	}
	want := map[string]any{
		"kafka.bootstrap":                []string{"env-1:9092", "env-2:9092"},
		"http.read_header_timeout":       3 * time.Second,
		"http.body_limit":                3 * MiB,
		"http.tls":                       ListenerTLS{CertFile: "/env/http.pem", KeyFile: "/env/http-key.pem"},
		"policy.read_only_mode":          false,
		"http.trusted_proxies":           []string{"192.168.0.0/16", "172.16.0.0/12"},
		"bounds.scan.max_matches":        Range[int]{Default: 10, Ceiling: 333},
		"auth.keys":                      []Key{{ID: "env-key", Tier: TierReader, SHA256: digestB}},
		"http.cors.origins":              []string{"https://a.example", "https://b.example"},
		"http.docs.enabled":              true,
		"kafka.consumer.isolation_level": "read_committed",
		"audit.topic":                    "env-audit",
		"kafka.sasl.mechanism":           "SCRAM-SHA-512",
		"http.addr":                      ":9090",
		"bounds.page.size":               Range[int]{Default: 25, Ceiling: 250},
		"telemetry.log_level":            "debug",
		"kafka.producer.acks":            "1",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("YAML then env precedence mismatch (-want +got):\n%s", diff)
	}
}

func TestLoad_UnknownYAMLKey_Fails(t *testing.T) {
	t.Parallel()
	_, err := Load(Options{Path: filepath.Join("testdata", "unknown_key.yaml"), Environ: []string{}})
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("Load() = %v, want ErrInvalid", err)
	}
	for _, k := range []string{"kafka.boostrap_typo", "http.read_header_timout"} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("error %q does not name unknown key %q", err, k)
		}
	}
}

func TestLoad_UnknownEnvVar_Fails(t *testing.T) {
	t.Parallel()
	_, err := Load(Options{
		Path:    filepath.Join("testdata", "minimal.yaml"),
		Environ: []string{"KGW_HTTP_READHEADER_TIMEOUT=1s", "KGW_NOPE=1"},
	})
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("Load() = %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "KGW_HTTP_READHEADER_TIMEOUT") || !strings.Contains(err.Error(), "KGW_NOPE") {
		t.Errorf("error %q does not name both unknown variables", err)
	}
}

func TestLoad_MinimalFileGetsDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load(Options{Path: filepath.Join("testdata", "minimal.yaml"), Environ: []string{}})
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	want := Default()
	want.Kafka.Bootstrap = []string{"localhost:9092"}
	want.Auth.Keys = []Key{{ID: "dev", Tier: TierOperator, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
	want.HTTP.WriteTimeout = want.Bounds.Scan.MaxTime.Ceiling + writeTimeoutSlack
	if diff := cmp.Diff(want, cfg); diff != "" {
		t.Errorf("minimal file (-want +got):\n%s", diff)
	}
}

func TestLoad_MissingFile_Fails(t *testing.T) {
	t.Parallel()
	if _, err := Load(Options{Path: filepath.Join("testdata", "does-not-exist.yaml"), Environ: []string{}}); err == nil {
		t.Fatal("Load() = nil error for a missing file")
	}
}

func TestPathFromArgs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []string
		env  []string
		want string
	}{
		{"flag separate", []string{"serve", "--config", "a.yaml"}, nil, "a.yaml"},
		{"flag equals", []string{"--config=b.yaml"}, nil, "b.yaml"},
		{"env fallback", []string{"serve"}, []string{"KGW_CONFIG=c.yaml"}, "c.yaml"},
		{"flag beats env", []string{"--config=a.yaml"}, []string{"KGW_CONFIG=c.yaml"}, "a.yaml"},
		{"none", []string{"serve"}, []string{"HOME=/x"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := PathFromArgs(tc.args, tc.env); got != tc.want {
				t.Errorf("PathFromArgs() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidate_DuplicateKeyID_Fails(t *testing.T) {
	t.Parallel()
	c := valid()
	c.Auth.Keys = append(c.Auth.Keys, Key{ID: "ops", Tier: TierReader, SHA256: digestB})
	expectInvalid(t, &c, `duplicate id "ops"`)
}

func TestValidate_BadDigest_Fails(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"", "abc", digestA[:63], digestA + "0", "zz" + digestA[2:]} {
		c := valid()
		c.Auth.Keys[0].SHA256 = bad
		expectInvalid(t, &c, "auth.keys[0].sha256")
	}
	// Upper-case hex is accepted and normalised to lower case.
	c := valid()
	c.Auth.Keys[0].SHA256 = strings.ToUpper("abcdef" + digestA[6:])
	mustValidate(t, &c)
	if c.Auth.Keys[0].SHA256 != "abcdef"+digestA[6:] {
		t.Errorf("digest not normalised: %q", c.Auth.Keys[0].SHA256)
	}
}

func TestValidate_UnknownTier_Fails(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"", "admin", "Reader", "OPERATOR"} {
		c := valid()
		c.Auth.Keys[0].Tier = bad
		expectInvalid(t, &c, "auth.keys[0].tier")
	}
}

func TestValidate_AuthEnabledWithoutKeys_Fails(t *testing.T) {
	t.Parallel()
	c := valid()
	c.Auth.Keys = nil
	expectInvalid(t, &c, "auth.keys: at least one key")
	c.Auth.Enabled = false
	mustValidate(t, &c)
}

func TestValidate_CeilingBelowDefault_Fails(t *testing.T) {
	t.Parallel()
	cases := []struct {
		path   string
		mutate func(*Config)
	}{
		{"bounds.read.limit", func(c *Config) { c.Bounds.Read.Limit.Ceiling = 99 }},
		{"bounds.scan.max_scan", func(c *Config) { c.Bounds.Scan.MaxScan.Ceiling = 1 }},
		{"bounds.scan.max_matches", func(c *Config) { c.Bounds.Scan.MaxMatches.Ceiling = 99 }},
		{"bounds.scan.max_bytes", func(c *Config) { c.Bounds.Scan.MaxBytes.Ceiling = 1 * MiB }},
		{"bounds.scan.max_time", func(c *Config) { c.Bounds.Scan.MaxTime.Ceiling = time.Second }},
		{"bounds.replay.limit", func(c *Config) { c.Bounds.Replay.Limit.Ceiling = 999 }},
		{"bounds.throughput.seconds", func(c *Config) { c.Bounds.Throughput.Seconds.Ceiling = 4 }},
		{"bounds.page.size", func(c *Config) { c.Bounds.Page.Size.Ceiling = 49 }},
		{"bounds.read.limit.default", func(c *Config) { c.Bounds.Read.Limit.Default = 0 }},
		{"bounds.scan.regex_timeout", func(c *Config) { c.Bounds.Scan.RegexTimeout = 0 }},
		{"bounds.bulk_produce_body", func(c *Config) { c.Bounds.BulkProduceBody = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			c := valid()
			tc.mutate(&c)
			expectInvalid(t, &c, tc.path)
		})
	}
}

func TestValidate_WriteTimeoutDerivedFromMaxTime(t *testing.T) {
	t.Parallel()
	c := valid()
	c.Bounds.Scan.MaxTime.Ceiling = 42 * time.Second
	mustValidate(t, &c)
	if want := 47 * time.Second; c.HTTP.WriteTimeout != want {
		t.Errorf("derived WriteTimeout = %s, want %s", c.HTTP.WriteTimeout, want)
	}

	explicit := valid()
	explicit.HTTP.WriteTimeout = 5 * time.Minute
	mustValidate(t, &explicit)
	if explicit.HTTP.WriteTimeout != 5*time.Minute {
		t.Errorf("explicit WriteTimeout overwritten: %s", explicit.HTTP.WriteTimeout)
	}

	tooLow := valid()
	tooLow.HTTP.WriteTimeout = time.Second
	expectInvalid(t, &tooLow, "http.write_timeout")
}

func TestValidate_CORSWildcardWithAuth_Fails(t *testing.T) {
	t.Parallel()
	c := valid()
	c.HTTP.CORS.Origins = []string{"https://ui.example", "*"}
	expectInvalid(t, &c, "http.cors.origins: wildcard")
	c.Auth.Enabled = false
	mustValidate(t, &c)
}

func TestValidate_OtherRules(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no bootstrap", func(c *Config) { c.Kafka.Bootstrap = nil }, "kafka.bootstrap: at least one"},
		{"bad bootstrap", func(c *Config) { c.Kafka.Bootstrap = []string{"nohostport"} }, "kafka.bootstrap"},
		{"sasl needs creds", func(c *Config) { c.Kafka.SASL.Mechanism = "PLAIN" }, "kafka.sasl: username and password"},
		{"sasl unknown", func(c *Config) { c.Kafka.SASL.Mechanism = "GSSAPI" }, "kafka.sasl.mechanism"},
		{"acks", func(c *Config) { c.Kafka.Producer.Acks = "2" }, "kafka.producer.acks"},
		{"isolation", func(c *Config) { c.Kafka.Consumer.IsolationLevel = "x" }, "kafka.consumer.isolation_level"},
		{"kafka tls pair", func(c *Config) { c.Kafka.TLS.Enabled = true; c.Kafka.TLS.CertFile = "c" }, "kafka.tls"},
		{"http tls pair", func(c *Config) { c.HTTP.TLS.KeyFile = "k" }, "http.tls"},
		{"bad cidr", func(c *Config) { c.HTTP.TrustedProxies = []string{"10.0.0.1"} }, "http.trusted_proxies"},
		{"op id shape", func(c *Config) { c.Policy.DisabledOperations = []string{"t11"} }, "policy.disabled_operations"},
		{"op id dup", func(c *Config) { c.Policy.DisabledOperations = []string{"T11", "T11"} }, "duplicate"},
		{"audit sink", func(c *Config) { c.Audit.Sink = "file" }, "audit.sink"},
		{"audit topic", func(c *Config) { c.Audit.Sink = AuditSinkKafka }, "audit.topic: required"},
		{"otlp protocol", func(c *Config) { c.Telemetry.OTLP.Protocol = "thrift" }, "telemetry.otlp.protocol"},
		{"log level", func(c *Config) { c.Telemetry.LogLevel = "trace" }, "telemetry.log_level"},
		{"service name", func(c *Config) { c.Telemetry.ServiceName = "" }, "telemetry.service_name"},
		{"request timeout", func(c *Config) { c.Kafka.RequestTimeout = 0 }, "kafka.request_timeout"},
		{"http addr", func(c *Config) { c.HTTP.Addr = "" }, "http.addr"},
		{"read header timeout", func(c *Config) { c.HTTP.ReadHeaderTimeout = -1 }, "http.read_header_timeout"},
		{"idle timeout", func(c *Config) { c.HTTP.IdleTimeout = 0 }, "http.idle_timeout"},
		{"body limit", func(c *Config) { c.HTTP.BodyLimit = 0 }, "http.body_limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := valid()
			tc.mutate(&c)
			expectInvalid(t, &c, tc.want)
		})
	}
}

func TestValidate_ReportsAllErrorsAtOnce(t *testing.T) {
	t.Parallel()
	c := valid()
	c.Kafka.Bootstrap = nil
	c.Audit.Sink = "nope"
	c.Auth.Keys[0].Tier = "root"
	err := Validate(&c)
	if err == nil {
		t.Fatal("Validate() = nil")
	}
	for _, want := range []string{"kafka.bootstrap", "audit.sink", "auth.keys[0].tier"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestRedact_SecretsMasked(t *testing.T) {
	t.Parallel()
	c := valid()
	c.Kafka.SASL.Mechanism, c.Kafka.SASL.Username, c.Kafka.SASL.Password = "PLAIN", "gw", "hunter2-sasl"
	c.Kafka.TLS.Enabled, c.Kafka.TLS.CertFile, c.Kafka.TLS.KeyFile = true, "/tls/c.pem", "/tls/kafka-private.key"
	c.HTTP.TLS.CertFile, c.HTTP.TLS.KeyFile = "/tls/h.pem", "/tls/http-private.key"
	c.Auth.Keys = append(c.Auth.Keys, Key{ID: "ui", Tier: TierReader, SHA256: digestB})
	secrets := []string{"hunter2-sasl", "kafka-private.key", "http-private.key", digestA, digestB}

	var logBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&logBuf, nil)).Info("boot", "config", c)

	for name, out := range map[string]string{"String()": c.String(), "slog": logBuf.String()} {
		for _, s := range secrets {
			if strings.Contains(out, s) {
				t.Errorf("%s leaks %q:\n%s", name, s, out)
			}
		}
		if !strings.Contains(out, masked) {
			t.Errorf("%s contains no mask marker:\n%s", name, out)
		}
		// Non-secret identifiers stay visible so the dump is still useful.
		for _, keep := range []string{"gw", "ops", "ui", "/tls/c.pem", "localhost:9092"} {
			if !strings.Contains(out, keep) {
				t.Errorf("%s dropped non-secret %q:\n%s", name, keep, out)
			}
		}
	}
	if c.Kafka.SASL.Password != "hunter2-sasl" || c.Auth.Keys[0].SHA256 != digestA {
		t.Error("Redacted() mutated the original Config")
	}
}

func TestDefaults_MatchSpecTable(t *testing.T) {
	t.Parallel()
	d := Default()
	got := map[string]any{
		"M1 limit":              d.Bounds.Read.Limit,
		"M3/M4 maxScan":         d.Bounds.Scan.MaxScan.Default,
		"M3/M4 maxBytes":        d.Bounds.Scan.MaxBytes.Default,
		"M3/M4 maxTime":         d.Bounds.Scan.MaxTime.Default,
		"M3/M4 maxMatches":      d.Bounds.Scan.MaxMatches,
		"M8 limit":              d.Bounds.Replay.Limit,
		"M6 body ceiling":       d.Bounds.BulkProduceBody,
		"C10 seconds":           d.Bounds.Throughput.Seconds,
		"regex timeout":         d.Bounds.Scan.RegexTimeout,
		"pageSize":              d.Bounds.Page.Size,
		"http read header":      d.HTTP.ReadHeaderTimeout,
		"http idle":             d.HTTP.IdleTimeout,
		"http body limit":       d.HTTP.BodyLimit,
		"producer acks":         d.Kafka.Producer.Acks,
		"producer idempotent":   d.Kafka.Producer.Idempotent,
		"isolation level":       d.Kafka.Consumer.IsolationLevel,
		"auth enabled":          d.Auth.Enabled,
		"docs enabled":          d.HTTP.Docs.Enabled,
		"audit sink":            d.Audit.Sink,
		"read-only / lock off":  d.Policy.ReadOnlyMode || d.Policy.DataPlaneLock,
		"disabled ops empty":    len(d.Policy.DisabledOperations),
		"telemetry protocol":    d.Telemetry.OTLP.Protocol,
		"telemetry serviceName": d.Telemetry.ServiceName,
	}
	want := map[string]any{
		"M1 limit":              Range[int]{Default: 100, Ceiling: 1000},
		"M3/M4 maxScan":         10_000,
		"M3/M4 maxBytes":        10 * MiB,
		"M3/M4 maxTime":         10 * time.Second,
		"M3/M4 maxMatches":      Range[int]{Default: 100, Ceiling: 1000},
		"M8 limit":              Range[int]{Default: 1000, Ceiling: 10_000},
		"M6 body ceiling":       10 * MiB,
		"C10 seconds":           Range[int]{Default: 5, Ceiling: 60},
		"regex timeout":         100 * time.Millisecond,
		"pageSize":              Range[int]{Default: 50, Ceiling: 500},
		"http read header":      10 * time.Second,
		"http idle":             60 * time.Second,
		"http body limit":       1 * MiB,
		"producer acks":         "all",
		"producer idempotent":   true,
		"isolation level":       "read_committed",
		"auth enabled":          true,
		"docs enabled":          true,
		"audit sink":            AuditSinkStdout,
		"read-only / lock off":  false,
		"disabled ops empty":    0,
		"telemetry protocol":    "grpc",
		"telemetry serviceName": "kafka3o-gateway",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Default() vs FUNC-SPEC §8.8 / TECH-SPEC C4, C6 (-want +got):\n%s", diff)
	}
}

func TestExampleConfig_LoadsAndListsEveryKey(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "configs", "config.example.yaml")

	// It must parse and, with the two required values supplied, validate.
	cfg, err := Load(Options{Path: path, Environ: []string{
		"KGW_KAFKA_BOOTSTRAP=localhost:9092",
		`KGW_API_KEYS=[{"id":"x","tier":"reader","sha256":"` + digestA + `"}]`,
	}})
	if err != nil {
		t.Fatalf("Load(config.example.yaml) error: %v", err)
	}

	// Every value in the example equals the coded default (the file documents Default()).
	want := Default()
	want.Kafka.Bootstrap = []string{"localhost:9092"}
	want.Auth.Keys = []Key{{ID: "x", Tier: TierReader, SHA256: digestA}}
	want.HTTP.WriteTimeout = want.Bounds.Scan.MaxTime.Ceiling + writeTimeoutSlack
	if diff := cmp.Diff(want, cfg, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("config.example.yaml values differ from Default() (-want +got):\n%s", diff)
	}

	// Every leaf path of Config appears in the file — no key is undocumented.
	present, err := yamlLeafPaths(path)
	if err != nil {
		t.Fatalf("yamlLeafPaths() error: %v", err)
	}
	for _, p := range Paths() {
		if !present[p] {
			t.Errorf("config.example.yaml is missing key %q", p)
		}
	}
	for p := range present {
		if _, ok := schema().leaves[p]; !ok {
			t.Errorf("config.example.yaml has key %q that Config does not define", p)
		}
	}
}

// TestOperationsDoc_DocumentsEveryKey keeps docs/operations.md in step with
// Config: every key, and every operations topic TECH-SPEC §5.1 names.
func TestOperationsDoc_DocumentsEveryKey(t *testing.T) {
	t.Parallel()
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatalf("read docs/operations.md: %v", err)
	}
	text := string(doc)
	for _, p := range Paths() {
		if !strings.Contains(text, "`"+p+"`") {
			t.Errorf("docs/operations.md does not document key %q", p)
		}
	}
	for _, section := range []string{
		"## Supported brokers", "## Command line", "## Health probes", "## Shutdown",
		"## Audit topic provisioning", "## Break-glass", "## DELETE requests carry a JSON body",
	} {
		if !strings.Contains(text, section) {
			t.Errorf("docs/operations.md is missing section %q", section)
		}
	}
}

func TestByteSize_ParseAndFormat(t *testing.T) {
	t.Parallel()
	cases := map[string]ByteSize{
		"0": 0, "1024": 1024, "1KiB": KiB, "10MiB": 10 * MiB, "2GiB": 2 * GiB,
		"3MB": 3 * MiB, " 7 KiB ": 7 * KiB, "12B": 12,
	}
	for in, want := range cases {
		var b ByteSize
		if err := b.UnmarshalText([]byte(in)); err != nil || b != want {
			t.Errorf("UnmarshalText(%q) = %v, %v; want %v", in, b, err, want)
		}
	}
	for _, bad := range []string{"", "-1", "1.5MiB", "MiB", "1TiB", "ten"} {
		var b ByteSize
		if err := b.UnmarshalText([]byte(bad)); err == nil {
			t.Errorf("UnmarshalText(%q) = nil error", bad)
		}
	}
	for want, in := range map[string]ByteSize{"1MiB": MiB, "3KiB": 3 * KiB, "1GiB": GiB, "1500B": 1500} {
		if got := in.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int64(in), got, want)
		}
	}
}

func TestEnvName_MatchesSpecNames(t *testing.T) {
	t.Parallel()
	// TECH-SPEC §6.3 names that follow the tree; aliases are covered by TestLoad_YAMLThenEnvOverrides.
	cases := map[string]string{
		"http.trusted_proxies":     "KGW_HTTP_TRUSTED_PROXIES",
		"http.tls.cert_file":       "KGW_HTTP_TLS_CERT_FILE",
		"http.read_header_timeout": "KGW_HTTP_READ_HEADER_TIMEOUT",
		"http.idle_timeout":        "KGW_HTTP_IDLE_TIMEOUT",
		"http.body_limit":          "KGW_HTTP_BODY_LIMIT",
		"audit.topic":              "KGW_AUDIT_TOPIC",
		"kafka.producer.acks":      "KGW_KAFKA_PRODUCER_ACKS",
	}
	for path, want := range cases {
		if got := EnvName(path); got != want {
			t.Errorf("EnvName(%q) = %q, want %q", path, got, want)
		}
	}
	s := schema()
	for _, alias := range []string{
		"KGW_API_KEYS",
		"KGW_CORS_ORIGINS",
		"KGW_DOCS_ENABLED",
		"KGW_HTTP_TLS_CERT",
		"KGW_HTTP_TLS_KEY",
		"KGW_KAFKA_ISOLATION_LEVEL",
	} {
		if _, ok := s.byEnv[alias]; !ok {
			t.Errorf("alias %s is not mapped", alias)
		}
	}
}
