package main

import (
	"bufio"
	"bytes"
	"context"
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().String()
	origin := "https://" + address
	now := time.Now()
	cert, key, _ := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	tlsConfig, err := loadServerTLS(serverConfig{origin: origin, certificate: cert, key: key})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api, err := newSessionHandler(db, origin, logger)
	if err != nil {
		t.Fatal(err)
	}
	files := http.FileServer(http.Dir(dist))
	mux := http.NewServeMux()
	mux.Handle("/api/", api)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
		} else if r.URL.Path == "/" {
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
		Origin string `json:"origin"`
		CAFile string `json:"caFile"`
	}{origin, filepath.Join(filepath.Dir(cert), "ca.pem")})
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
