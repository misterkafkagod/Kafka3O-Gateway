package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestConsolePreviewHTTPSFixture(t *testing.T) {
	if os.Getenv("KAFKA3O_TEST_CONSOLE_PREVIEW") != "1" {
		t.Skip("the cross-repository preview fixture is opt-in")
	}
	dist := os.Getenv("KAFKA3O_TEST_CONSOLE_PREVIEW_DIST")
	if !filepath.IsAbs(dist) {
		t.Fatal("the preview build path must be absolute")
	}
	if info, err := os.Stat(filepath.Join(dist, "index.html")); err != nil || !info.Mode().IsRegular() {
		t.Fatal("the preview build must contain index.html")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "preview.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	input := strings.NewReader(secret + "\nadmin\ninitial passphrase")
	if err := runBootstrap(ctx, path, secret, input); err != nil {
		t.Fatal(err)
	}
	if err := runBootstrap(ctx, path, secret, strings.NewReader(secret+"\nother\nanother passphrase")); !errors.Is(err, identity.ErrAlreadyBootstrapped) {
		t.Fatalf("reused synthetic bootstrap: %v", err)
	}
	db, err := storage.OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close synthetic Console store: %v", err)
		}
	})
	if err := db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatal(err)
	}
	// Optional synthetic registry for the administration preview test; no Gateway is contacted.
	if os.Getenv("KAFKA3O_TEST_CONSOLE_PREVIEW_CLUSTERS") == "1" {
		if err := db.SyncRegisteredClusters(ctx, []identity.RegisteredCluster{
			{ID: "dev", Name: "Dev", GatewayURL: "https://dev.invalid:8081"},
			{ID: "prod", Name: "Prod", GatewayURL: "https://prod.invalid:8082"},
		}, "preview-registry", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	// Optional Cognito: "synthetic" or "synthetic-outage" use the synthetic HTTPS provider;
	// "environment" reads KAFKA3O_CONSOLE_COGNITO_* for a real test User Pool on https://localhost:8443.
	mode := os.Getenv("KAFKA3O_TEST_CONSOLE_PREVIEW_COGNITO")
	if mode != "" && mode != "synthetic" && mode != "synthetic-outage" && mode != "environment" {
		t.Fatal("unknown preview Cognito mode")
	}
	listen, host := "127.0.0.1:0", "127.0.0.1"
	if mode == "environment" {
		listen, host = "127.0.0.1:8443", "localhost"
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	address := net.JoinHostPort(host, port)
	origin := "https://" + address
	now := time.Now()
	cert, key, _ := serverCertificate(t, host, now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	tlsConfig, err := loadServerTLS(serverConfig{origin: origin, certificate: cert, key: key})
	if err != nil {
		t.Fatal(err)
	}
	var logs io.Writer = io.Discard
	if mode != "" {
		logs = os.Stderr // scanned by the UI integration test for leaked secrets
	}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	api, err := newSessionHandler(db, origin, logger)
	if err != nil {
		t.Fatal(err)
	}
	var providerCAFile string
	if mode != "" {
		var config cognitoConfig
		if mode == "environment" {
			config, _, err = loadCognitoConfig(os.Getenv, origin)
		} else {
			provider := newSyntheticCognitoProvider(t)
			providerCAFile = provider.caFile
			provider.issuedAge.Store(int64(5*time.Minute - 10*time.Second))
			if mode == "synthetic-outage" {
				provider.discoveryStatus.Store(http.StatusServiceUnavailable)
			}
			values := provider.environment()
			config, _, err = loadCognitoConfig(func(name string) string { return values[name] }, origin)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := api.EnableCognito(rand.Reader); err != nil {
			t.Fatal(err)
		}
		discovered := make(chan struct{})
		defer func() { cancel(); <-discovered }()
		go func() {
			defer close(discovered)
			discoverCognito(ctx, func(ctx context.Context) (consoleapi.CognitoServices, error) {
				return newCognitoServices(ctx, db, config, rand.Reader, time.Now)
			}, api.PublishCognito, func(int) <-chan time.Time { return time.After(time.Second) }, logger)
		}()
	}
	files := http.FileServer(http.Dir(dist))
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
		} else if r.URL.Path == "/" || r.URL.Path == "/auth/cognito/callback" {
			http.ServeFile(w, r, filepath.Join(dist, "index.html"))
		} else if strings.HasPrefix(r.URL.Path, "/assets/") {
			files.ServeHTTP(w, r)
		} else {
			http.NotFound(w, r)
		}
	})
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.TLS == nil || r.Host != address {
				http.Error(w, "invalid origin", http.StatusForbidden)
				return
			}
			mux.ServeHTTP(w, r)
		}),
		TLSConfig: tlsConfig, ErrorLog: log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	ready, err := json.Marshal(struct {
		Origin         string `json:"origin"`
		CAFile         string `json:"caFile"`
		ProviderCAFile string `json:"providerCAFile,omitempty"`
	}{origin, filepath.Join(filepath.Dir(cert), "ca.pem"), providerCAFile})
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				stopped <- fmt.Errorf("read fixture shutdown: %w", err)
			} else {
				stopped <- errors.New("fixture shutdown input closed unexpectedly")
			}
		} else if scanner.Text() != "stop" {
			stopped <- errors.New("invalid fixture shutdown request")
		} else {
			stopped <- nil
		}
		cancel()
	}()
	fmt.Printf("KAFKA3O_PREVIEW_READY=%s\n", ready)
	if err := serveConsole(ctx, server, listener); err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}
