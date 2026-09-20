//go:build acceptance

// Package acceptance is the gateway's Level 2 acceptance suite (TECH-SPEC
// §4.8, T2): it starts the real gateway in-process — internal/app.Run with
// the franz adapter — against a dedicated, non-production cluster, then
// drives it over HTTP exactly as a real client would. It is human-triggered
// per release, never part of the automated PR pipeline, and needs
// environment variables no default supplies (see envOrFatal below).
package acceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/app"
	"github.com/misterkafkagod/kafka3o/internal/config"
)

// runID tags every resource this run creates (TECH-SPEC §4.8 isolation):
// acc-<runID>-<name>, so concurrent or repeated runs never collide.
var runID string

// baseURL and the two presented (unhashed) API keys the acceptance HTTP
// client uses; TestMain sets these up before any test runs.
var (
	baseURL     string
	operatorKey string
	readerKey   string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	runID = strconv.FormatInt(time.Now().UnixNano(), 36)

	operatorKey = envOrFatal("KGW_ACC_OPERATOR_KEY")
	readerKey = envOrFatal("KGW_ACC_READER_KEY")

	cfg, err := acceptanceConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "test/acceptance: build config:", err)
		return 1
	}
	baseURL = "http://" + cfg.HTTP.Addr

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- app.Run(ctx, cfg) }()

	if err := waitLive(30 * time.Second); err != nil {
		cancel()
		fmt.Fprintln(os.Stderr, "test/acceptance: gateway never became live:", err)
		return 1
	}

	code := m.Run()

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			fmt.Fprintln(os.Stderr, "test/acceptance: app.Run:", err)
		}
	case <-time.After(15 * time.Second):
		fmt.Fprintln(os.Stderr, "test/acceptance: gateway did not shut down in time")
	}
	return code
}

// acceptanceConfig builds the Config for this run (TECH-SPEC §4.8 cluster
// prerequisites): KAFKA_BOOTSTRAP plus TLS/SASL environment, the operator
// and reader keys, every per-operation switch enabled, read-only mode and
// the data-plane lock off, audit to stdout.
func acceptanceConfig() (config.Config, error) {
	cfg := config.Default()
	cfg.HTTP.Addr = envOr("KGW_ACC_HTTP_ADDR", "127.0.0.1:18080")

	cfg.Kafka.Bootstrap = strings.Split(envOrFatal("KAFKA_BOOTSTRAP"), ",")
	if os.Getenv("KAFKA_TLS_ENABLED") == "true" {
		cfg.Kafka.TLS = config.KafkaTLS{
			Enabled:  true,
			CAFile:   os.Getenv("KAFKA_TLS_CA_FILE"),
			CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
			KeyFile:  os.Getenv("KAFKA_TLS_KEY_FILE"),
		}
	}
	if mech := os.Getenv("KAFKA_SASL_MECHANISM"); mech != "" {
		cfg.Kafka.SASL = config.SASL{
			Mechanism: mech,
			Username:  os.Getenv("KAFKA_SASL_USERNAME"),
			Password:  os.Getenv("KAFKA_SASL_PASSWORD"),
		}
	}

	cfg.Auth.Enabled = true
	cfg.Auth.Keys = []config.Key{
		{ID: "acceptance-operator", Tier: config.TierOperator, SHA256: digestHex(operatorKey)},
		{ID: "acceptance-reader", Tier: config.TierReader, SHA256: digestHex(readerKey)},
	}
	cfg.Policy = config.Policy{} // every switch off: nothing disabled, not read-only, lock off
	cfg.Audit = config.Audit{Sink: config.AuditSinkStdout}

	if err := config.Validate(&cfg); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}

// digestHex is the SHA-256 digest config.Key.SHA256 expects (TECH-SPEC B4).
func digestHex(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// waitLive polls /health/live until it answers 200 or timeout elapses —
// the only bounded wait in this suite that cannot be expressed as an
// injected clock, since it is waiting on a real process to start listening.
func waitLive(timeout time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/health/live", nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Api-Key", operatorKey)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("status %d", resp.StatusCode)
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s: %w", timeout, lastErr)
}

// envOr returns the value of name, or def if it is unset.
func envOr(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

// envOrFatal returns the value of name, or aborts the whole run — this
// suite cannot proceed without a real cluster and real credentials.
func envOrFatal(name string) string {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		fmt.Fprintf(os.Stderr, "test/acceptance: %s is required\n", name)
		os.Exit(1)
	}
	return v
}
