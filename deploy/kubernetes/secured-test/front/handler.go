package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	browserHost   = "localhost:5173"
	browserOrigin = "http://localhost:5173"
)

// newConsoleRelay forwards /api/* to the Console's HTTPS loopback origin, verifying its certificate with roots.
// The browser's Origin was already checked; upstream sees the Console's own origin.
func newConsoleRelay(consoleOrigin string, roots *x509.CertPool) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(consoleOrigin)
	if err != nil || target.Scheme != "https" || target.Host == "" || target.Path != "" {
		return nil, errors.New("the Console origin must be an HTTPS origin")
	}
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: target.Hostname()},
		ResponseHeaderTimeout: 20 * time.Second,
		MaxIdleConnsPerHost:   16,
	}
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = target.Host
			if r.In.Header.Get("Origin") != "" {
				r.Out.Header.Set("Origin", consoleOrigin)
			}
			for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-Ip"} {
				r.Out.Header.Del(header)
			}
		},
		Transport: transport,
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode >= 300 && response.StatusCode < 400 {
				return errors.New("the Console must not redirect")
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"title":"The Console is unavailable","status":502,"code":"console_unavailable"}` + "\n"))
		},
	}, nil
}

// newFrontHandler serves the secured UI and relays /api to the Console for the forwarded loopback origin only.
func newFrontHandler(directory string, relay http.Handler) http.Handler {
	files := http.FileServer(http.Dir(directory))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if r.Host != browserHost {
			http.Error(w, "unexpected host", http.StatusMisdirectedRequest)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && origin != browserOrigin {
			http.Error(w, "unexpected origin", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
			relay.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'")
		clean := path.Clean("/" + r.URL.Path)
		if strings.HasPrefix(clean, "/assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			files.ServeHTTP(w, r)
			return
		}
		if info, err := os.Stat(filepath.Join(directory, filepath.FromSlash(clean))); clean != "/" && err == nil && info.Mode().IsRegular() {
			w.Header().Set("Cache-Control", "no-cache")
			files.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFile(w, r, filepath.Join(directory, "index.html"))
	})
}
