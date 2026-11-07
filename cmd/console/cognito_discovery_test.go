package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type lockedBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

type cognitoDiscoveryFixture struct {
	provider *syntheticCognitoProvider
	api      *consoleapi.SessionAPI
	db       *storage.Database
	config   cognitoConfig
	logs     *lockedBuffer
	logger   *slog.Logger
	attempts chan error
	retries  chan time.Time
}

func newCognitoDiscoveryFixture(t *testing.T) *cognitoDiscoveryFixture {
	t.Helper()
	provider := newSyntheticCognitoProvider(t)
	db, err := storage.OpenDatabase(context.Background(), filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	api, err := newSessionHandler(db, cognitoTestOrigin, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.EnableCognito(rand.Reader); err != nil {
		t.Fatal(err)
	}
	values := provider.environment()
	config, _, err := loadCognitoConfig(func(name string) string { return values[name] }, cognitoTestOrigin)
	if err != nil {
		t.Fatal(err)
	}
	return &cognitoDiscoveryFixture{provider: provider, api: api, db: db, config: config, logs: logs, logger: logger,
		attempts: make(chan error, 16), retries: make(chan time.Time)}
}

func (f *cognitoDiscoveryFixture) run(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		discoverCognito(ctx, func(ctx context.Context) (consoleapi.CognitoServices, error) {
			services, err := newCognitoServices(ctx, f.db, f.config, rand.Reader, time.Now)
			f.attempts <- err
			return services, err
		}, f.api.PublishCognito, func(int) <-chan time.Time { return f.retries }, f.logger)
	}()
	return done
}

func (f *cognitoDiscoveryFixture) available(t *testing.T) bool {
	t.Helper()
	response := httptest.NewRecorder()
	f.api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, cognitoTestOrigin+"/api/session/cognito", nil))
	var body struct {
		Available *bool `json:"available"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil || body.Available == nil {
		t.Fatalf("Cognito availability status=%d", response.Code)
	}
	return *body.Available
}

func waitForDiscoveryAttempt(t *testing.T, attempts <-chan error) error {
	t.Helper()
	select {
	case err := <-attempts:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("Cognito discovery did not attempt")
		return nil
	}
}

func TestCognitoDiscoveryRecoversAfterOutageWhileLocalLoginServes(t *testing.T) {
	f := newCognitoDiscoveryFixture(t)
	f.provider.discoveryStatus.Store(http.StatusServiceUnavailable)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := f.run(ctx)
	if err := waitForDiscoveryAttempt(t, f.attempts); !errors.Is(err, cognito.ErrProviderUnavailable) {
		t.Fatalf("provider outage was not reported as unavailable: %v", err)
	}
	if f.available(t) {
		t.Fatal("Cognito was available during a provider outage")
	}
	login := httptest.NewRequest(http.MethodPost, cognitoTestOrigin+"/api/session/local",
		strings.NewReader(`{"username":"nobody","password":"not a real passphrase"}`))
	login.Header.Set("Origin", cognitoTestOrigin)
	login.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	f.api.ServeHTTP(response, login)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("local login was not served during the provider outage: status=%d", response.Code)
	}
	f.provider.discoveryStatus.Store(0)
	f.retries <- time.Now()
	if err := waitForDiscoveryAttempt(t, f.attempts); err != nil {
		t.Fatalf("Cognito discovery did not recover: %v", err)
	}
	<-done
	if !f.available(t) {
		t.Fatal("Cognito was unavailable after discovery recovered")
	}
	logs := f.logs.String()
	if !strings.Contains(logs, "Cognito sign-in unavailable") || !strings.Contains(logs, "Cognito sign-in available") {
		t.Fatal("Cognito discovery outcome was not logged")
	}
	for _, secret := range []string{syntheticCognitoSecret, syntheticAttemptKey, syntheticRefreshKey, "synthetic-key"} {
		if strings.Contains(logs, secret) {
			t.Fatal("Cognito discovery logged secret or provider material")
		}
	}
}

func TestCognitoDiscoveryStopsOnCancellation(t *testing.T) {
	f := newCognitoDiscoveryFixture(t)
	f.provider.discoveryStatus.Store(http.StatusBadGateway)
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	waitForDiscoveryAttempt(t, f.attempts)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Cognito discovery ignored cancellation")
	}
	if f.available(t) {
		t.Fatal("cancelled discovery published Cognito")
	}
}

func TestCognitoDiscoveryReportsProviderMismatch(t *testing.T) {
	f := newCognitoDiscoveryFixture(t)
	f.provider.issuerMismatch.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := f.run(ctx)
	if err := waitForDiscoveryAttempt(t, f.attempts); !errors.Is(err, cognito.ErrInvalidProvider) {
		t.Fatalf("issuer mismatch was not rejected: %v", err)
	}
	// The warning is written after the attempt returns; wait for it before cancelling.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && !strings.Contains(f.logs.String(), "provider_mismatch=true"); {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if f.available(t) || !strings.Contains(f.logs.String(), "provider_mismatch=true") {
		t.Fatal("issuer mismatch was published or not reported")
	}
}

func TestCognitoDiscoveryPublishesOnce(t *testing.T) {
	f := newCognitoDiscoveryFixture(t)
	services, err := newCognitoServices(context.Background(), f.db, f.config, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(services.LogoutURL, f.provider.server.URL+"/logout?") ||
		!strings.Contains(services.LogoutURL, "client_id="+syntheticCognitoClientID) ||
		!strings.Contains(services.LogoutURL, "logout_uri=https%3A%2F%2Fconsole.example%2F") {
		t.Fatal("Cognito logout URL is not the allowlisted endpoint with the configured return")
	}
	if err := f.api.PublishCognito(services); err != nil {
		t.Fatal(err)
	}
	if err := f.api.PublishCognito(services); err == nil {
		t.Fatal("Cognito services were replaced after publication")
	}
	if err := f.api.EnableCognito(rand.Reader); err == nil {
		t.Fatal("Cognito was enabled twice")
	}
}

func TestCognitoDiscoveryDelayIsBounded(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, time.Minute, time.Minute}
	for attempt, delay := range want {
		if got := cognitoDiscoveryDelay(attempt); got != delay {
			t.Fatalf("attempt %d delay=%s, want %s", attempt, got, delay)
		}
	}
}

func TestCognitoConfigRejectedBeforeServing(t *testing.T) {
	directory := t.TempDir()
	certificate, key, _ := serverCertificate(t, "127.0.0.1", time.Now().Add(-time.Hour), time.Now().Add(time.Hour), x509.ExtKeyUsageServerAuth)
	base := serverEnvironment(filepath.Join(directory, "console.sqlite"), "127.0.0.1:8443", "https://127.0.0.1:8443", certificate, key)
	getenv := func(name string) string {
		if name == cognitoIssuerEnv {
			return "https://issuer.example/pool"
		}
		return base(name)
	}
	listened := false
	err := runServe(context.Background(), getenv, func(string, string) (net.Listener, error) {
		listened = true
		return nil, errors.New("unexpected listen")
	}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if !errors.Is(err, errInvalidCognitoConfig) || listened {
		t.Fatalf("partial Cognito configuration did not stop startup: listened=%t err=%v", listened, err)
	}
}
