package main

import (
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	listenAddress = "127.0.0.1:5173"
	uiDirectory   = "/usr/share/nginx/html"
)

var tokenPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--check" {
		if err := checkReady(); err != nil {
			log.Fatal(err)
		}
		return
	}
	if len(os.Args) != 1 {
		log.Fatal("usage: kafka3o-manual-proxy [--check]")
	}

	proxies := make(map[string]http.Handler)
	for _, gateway := range []struct {
		name, address, directory string
	}{
		{"a", "https://127.0.0.1:8081", "/run/gateway-a"},
		{"b", "https://127.0.0.1:8082", "/run/gateway-b"},
	} {
		token, roots, err := loadCredentials(gateway.directory)
		if err != nil {
			log.Fatalf("Gateway %s credentials: %v", gateway.name, err)
		}
		prefix := "/manual/gateways/" + gateway.name
		proxy, err := newGatewayProxy(prefix, gateway.address, token, roots)
		if err != nil {
			log.Fatalf("Gateway %s proxy: %v", gateway.name, err)
		}
		proxies[prefix] = proxy
	}

	server := &http.Server{
		Addr:              listenAddress,
		Handler:           newManualHandler(uiDirectory, proxies),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Fatal(server.ListenAndServe())
}

func loadCredentials(directory string) (string, *x509.CertPool, error) {
	rawToken, err := os.ReadFile(directory + "/token")
	if err != nil {
		return "", nil, fmt.Errorf("read token: %w", err)
	}
	token := strings.TrimSpace(string(rawToken))
	if !tokenPattern.MatchString(token) {
		return "", nil, errors.New("invalid test-token format")
	}

	certificate, err := os.ReadFile(directory + "/tls.crt")
	if err != nil {
		return "", nil, fmt.Errorf("read certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return "", nil, errors.New("invalid test certificate")
	}
	return token, roots, nil
}

func checkReady() error {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://localhost:5173/config.json")
	if err != nil {
		return fmt.Errorf("manual UI readiness: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("manual UI readiness: HTTP %d", response.StatusCode)
	}
	return nil
}
