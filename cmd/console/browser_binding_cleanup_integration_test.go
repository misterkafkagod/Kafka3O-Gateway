package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type browserCleanupSignalWriter struct {
	events chan string
}

func (w browserCleanupSignalWriter) Write(data []byte) (int, error) {
	for _, message := range []string{"Inactive browser bindings removed", "Browser binding cleanup unavailable"} {
		if bytes.Contains(data, []byte(message)) {
			select {
			case w.events <- message:
			default:
			}
		}
	}
	return len(data), nil
}

func TestBrowserBindingCleanupRunsInConsoleAndPreservesLocalLogin(t *testing.T) {
	for _, mode := range []string{"success", "audit failure"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			cert, key, roots := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
			path := filepath.Join(t.TempDir(), "console.sqlite")
			secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
			if err := runBootstrap(context.Background(), path, secret, strings.NewReader(secret+"\nadmin\ninitial passphrase")); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO identities (id, enabled) VALUES ('binding-fixture', 1);
INSERT INTO local_accounts (identity_id, username, password_hash, must_change_password, credential_version)
VALUES ('binding-fixture', 'binding-fixture', 'synthetic-hash', 0, 1)`); err != nil {
				t.Fatal(err)
			}
			for _, tt := range []struct {
				id          string
				token, hash byte
				idle        time.Time
			}{
				{"expired-binding", 1, 5, now.Add(-time.Hour)},
				{"active-binding", 2, 6, now.Add(time.Hour)},
			} {
				token := bytes.Repeat([]byte{tt.token}, 32)
				csrf := bytes.Repeat([]byte{tt.token + 2}, 32)
				hash := bytes.Repeat([]byte{tt.hash}, 32)
				if _, err := db.Exec(`INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, browser_binding_hash)
VALUES (?, 'binding-fixture', 1, ?, ?, ?, ?, ?, ?)`,
					tt.id, token, csrf, now.Add(-2*time.Hour).UnixNano(), tt.idle.UnixNano(), now.Add(2*time.Hour).UnixNano(), hash); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("INSERT INTO console_browser_sessions (browser_hash, session_id) VALUES (?, ?)", hash, tt.id); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "audit failure" {
				if _, err := db.Exec(`CREATE TRIGGER reject_browser_cleanup BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.browser.binding.cleanup'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END`); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			address := listener.Addr().String()
			origin := "https://" + address
			env := serverEnvironment(path, address, origin, cert, key)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			events := make(chan string, 2)
			logger := slog.New(slog.NewTextHandler(browserCleanupSignalWriter{events: events}, nil))
			go func() {
				done <- runServe(ctx, env, func(network, requested string) (net.Listener, error) {
					if network != "tcp" || requested != address {
						return nil, errors.New("unexpected listener")
					}
					return listener, nil
				}, logger)
			}()
			finished := false
			t.Cleanup(func() {
				if !finished {
					cancel()
					_ = listener.Close()
					<-done
				}
			})
			select {
			case event := <-events:
				if (mode == "success") != strings.Contains(event, "removed") {
					t.Fatalf("unexpected browser cleanup result: %s", event)
				}
			case err := <-done:
				finished = true
				t.Fatalf("Console stopped before browser cleanup: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("Console did not run its startup browser cleanup")
			}
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			request, err := http.NewRequest(http.MethodPost, origin+"/api/session/local",
				strings.NewReader(`{"username":"admin","password":"initial passphrase"}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Origin", origin)
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(io.Discard, response.Body)
			if closeErr := response.Body.Close(); err != nil || closeErr != nil {
				t.Fatalf("close login response: %v", errors.Join(err, closeErr))
			}
			if response.StatusCode != http.StatusOK {
				t.Fatalf("browser cleanup %s blocked local login: status=%d", mode, response.StatusCode)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Console did not stop cleanly: %v", err)
			}
			finished = true
			db, err = sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var expired, active, audits int
			var expiredRevoked, activeRevoked sql.NullInt64
			if err := db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM console_browser_sessions WHERE session_id = 'expired-binding'),
  (SELECT COUNT(*) FROM console_browser_sessions WHERE session_id = 'active-binding'),
  (SELECT COUNT(*) FROM audit_events WHERE action = 'console.browser.binding.cleanup'),
  (SELECT revoked_at_ns FROM console_sessions WHERE id = 'expired-binding'),
  (SELECT revoked_at_ns FROM console_sessions WHERE id = 'active-binding')`).
				Scan(&expired, &active, &audits, &expiredRevoked, &activeRevoked); err != nil {
				t.Fatal(err)
			}
			if active != 1 || activeRevoked.Valid {
				t.Fatal("startup cleanup damaged the active browser")
			}
			if mode == "success" && (expired != 0 || !expiredRevoked.Valid || audits != 1) {
				t.Fatal("startup browser cleanup did not persist its retirement and audit")
			}
			if mode == "audit failure" && (expired != 1 || expiredRevoked.Valid || audits != 0) {
				t.Fatal("audit failure committed a partial startup browser cleanup")
			}
		})
	}
}
