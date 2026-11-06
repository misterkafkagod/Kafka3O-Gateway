package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	expectedHost   = "localhost:5173"
	expectedOrigin = "http://" + expectedHost
	proxyHeader    = "X-Kafka3O-Manual"
)

func newGatewayProxy(prefix, address, token string, roots *x509.CertPool) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(address)
	if err != nil || target.Scheme != "https" || target.Hostname() != "127.0.0.1" ||
		target.User != nil || target.RawQuery != "" || target.Fragment != "" || target.Path != "" {
		return nil, fmt.Errorf("Gateway target must be an HTTPS loopback address")
	}
	if roots == nil || token == "" {
		return nil, fmt.Errorf("Gateway token and certificate trust are required")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "127.0.0.1",
	}
	transport.ResponseHeaderTimeout = 10 * time.Second

	return &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Path = strings.TrimPrefix(request.Out.URL.Path, prefix)
			if request.Out.URL.RawPath != "" {
				request.Out.URL.RawPath = strings.TrimPrefix(request.Out.URL.RawPath, prefix)
			}
			request.SetURL(target)
			request.Out.Host = target.Host
			request.Out.Header.Del(proxyHeader)
			request.Out.Header.Del("Cookie")
			request.Out.Header.Del("Origin")
			request.Out.Header.Del("Proxy-Authorization")
			request.Out.Header.Set("Authorization", "Bearer "+token)
		},
		ModifyResponse: func(response *http.Response) error {
			response.Header.Del("Set-Cookie")
			response.Header.Del("Access-Control-Allow-Origin")
			response.Header.Del("Access-Control-Allow-Credentials")
			response.Header.Del("Access-Control-Allow-Methods")
			response.Header.Del("Access-Control-Allow-Headers")
			if response.StatusCode == http.StatusUnauthorized {
				response.Body.Close()
				const body = `{"code":"manual_gateway_authentication_failed","detail":"The local test proxy could not authenticate to its Gateway. Check the manual deployment."}`
				response.StatusCode = http.StatusBadGateway
				response.Status = "502 Bad Gateway"
				response.Header.Set("Content-Type", "application/problem+json")
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				response.Header.Del("Content-Encoding")
				response.Header.Del("WWW-Authenticate")
				response.ContentLength = int64(len(body))
				response.TransferEncoding = nil
				response.Body = io.NopCloser(strings.NewReader(body))
				log.Printf("manual Gateway %s authentication failed", prefix)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("manual Gateway %s connection failed: %v", prefix, err)
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusBadGateway)
			io.WriteString(w, `{"code":"manual_gateway_unreachable","detail":"The local test proxy could not reach its Gateway. Check the manual deployment."}`)
		},
	}, nil
}

func newManualHandler(directory string, proxies map[string]http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; object-src 'none'; form-action 'self'")

		if r.Host != expectedHost || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != expectedOrigin) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/manual/gateways/") {
			for prefix, proxy := range proxies {
				apiPath := prefix + "/api/v1"
				if r.URL.Path != apiPath && !strings.HasPrefix(r.URL.Path, apiPath+"/") {
					continue
				}
				if r.Header.Get(proxyHeader) != "1" ||
					(r.Header.Get("Sec-Fetch-Site") != "" && r.Header.Get("Sec-Fetch-Site") != "same-origin") {
					http.Error(w, "Forbidden", http.StatusForbidden)
					return
				}
				switch r.Method {
				case http.MethodGet, http.MethodPost, http.MethodDelete:
					proxy.ServeHTTP(w, r)
				default:
					w.Header().Set("Allow", "GET, POST, DELETE")
					http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				}
				return
			}
			http.NotFound(w, r)
			return
		}

		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cleanPath := path.Clean("/" + r.URL.Path)
		filename := filepath.Join(directory, filepath.FromSlash(strings.TrimPrefix(cleanPath, "/")))
		info, err := os.Stat(filename)
		if err == nil && info.Mode().IsRegular() {
			http.ServeFile(w, r, filename)
			return
		}
		if err != nil && !os.IsNotExist(err) {
			log.Printf("manual UI file check failed: %v", err)
			http.Error(w, "Manual UI unavailable", http.StatusInternalServerError)
			return
		}
		if cleanPath == "/config.json" || strings.HasPrefix(cleanPath, "/assets/") {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(directory, "index.html"))
	})
}
