//go:build acceptance

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/app"
	"github.com/misterkafkagod/kafka3o/internal/config"
)

// runGatewayWithConfig starts a second, separately-configured gateway
// instance for a test needing a config switch (readOnlyMode, dataPlaneLock)
// the shared TestMain instance does not set (TECH-SPEC §4.8 "config-driven
// runs"), on its own port so it never conflicts with the shared one. It is
// shut down automatically when t completes.
func runGatewayWithConfig(t *testing.T, mutate func(*config.Config)) string {
	t.Helper()

	cfg, err := acceptanceConfig()
	if err != nil {
		t.Fatalf("acceptanceConfig() error: %v", err)
	}
	cfg.HTTP.Addr = freeAddr(t)
	mutate(&cfg)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- app.Run(ctx, cfg) }()

	url := "http://" + cfg.HTTP.Addr
	if err := waitGatewayLive(url, 30*time.Second); err != nil {
		cancel()
		t.Fatalf("second gateway never became live: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(15 * time.Second):
			fmt.Fprintln(os.Stderr, "test/acceptance: second gateway did not shut down in time")
		}
	})
	return url
}

// freeAddr asks the OS for an unused loopback port.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// waitGatewayLive is main_test.go's waitLive, parameterized over url instead
// of the package-level baseURL — this file's second gateway instance uses
// its own.
func waitGatewayLive(url string, timeout time.Duration) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url+"/v1/health/live", nil)
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

// doAt is doAcceptance against an explicit url instead of the shared
// baseURL, with optional extra headers and a JSON body.
func doAt(t *testing.T, url, method, path, apiKey string, extra map[string]string, body any) *http.Response {
	t.Helper()

	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("json.Marshal(body) error: %v", err)
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, url+path, reader)
	if err != nil {
		t.Fatalf("http.NewRequest(%s %q) error: %v", method, path, err)
	}
	req.Header.Set("X-Api-Key", apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range extra {
		req.Header.Set(name, value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s error: %v", method, path, err)
	}
	return resp
}

func TestAcceptance_F2_ReadOnlyMode(t *testing.T) {
	topic := acceptanceTopicOrSkip(t)
	url := runGatewayWithConfig(t, func(cfg *config.Config) { cfg.Policy.ReadOnlyMode = true })

	produceResp := doAt(t, url, http.MethodPost, "/v1/topics/"+topic+"/messages", operatorKey, nil, map[string]any{"value": "acceptance-f2-" + runID})
	defer drainAndClose(produceResp)
	if produceResp.StatusCode != http.StatusForbidden {
		t.Errorf("produce under read-only mode = %d, want 403", produceResp.StatusCode)
	}

	readResp := doAt(t, url, http.MethodGet, "/v1/topics/"+topic+"/messages?from=beginning&limit=1", operatorKey, nil, nil)
	defer drainAndClose(readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Errorf("read under read-only mode = %d, want 200", readResp.StatusCode)
	}
}

func TestAcceptance_F6_DataPlaneLockAndBreakGlass(t *testing.T) {
	topic := acceptanceTopicOrSkip(t)
	url := runGatewayWithConfig(t, func(cfg *config.Config) { cfg.Policy.DataPlaneLock = true })

	lockedResp := doAt(t, url, http.MethodGet, "/v1/topics/"+topic+"/messages?from=beginning&limit=1", operatorKey, nil, nil)
	defer drainAndClose(lockedResp)
	if lockedResp.StatusCode != http.StatusForbidden {
		t.Errorf("read under the lock, no break-glass = %d, want 403", lockedResp.StatusCode)
	}

	breakGlassResp := doAt(t, url, http.MethodGet, "/v1/topics/"+topic+"/messages?from=beginning&limit=1", operatorKey,
		map[string]string{"X-Break-Glass-Reason": "acceptance-" + runID}, nil)
	defer drainAndClose(breakGlassResp)
	if breakGlassResp.StatusCode != http.StatusOK {
		t.Errorf("read under the lock with break-glass = %d, want 200", breakGlassResp.StatusCode)
	}

	nonDataPlaneResp := doAt(t, url, http.MethodGet, "/v1/topics", operatorKey, nil, nil)
	defer drainAndClose(nonDataPlaneResp)
	if nonDataPlaneResp.StatusCode != http.StatusOK {
		t.Errorf("a non-data-plane route under the lock = %d, want 200", nonDataPlaneResp.StatusCode)
	}
}
