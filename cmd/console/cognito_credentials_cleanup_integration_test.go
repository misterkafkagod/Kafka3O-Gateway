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

type cleanupSignalWriter struct {
	events chan string
}

func (w cleanupSignalWriter) Write(data []byte) (int, error) {
	for _, message := range []string{"Inactive Cognito credentials removed", "Cognito credential cleanup unavailable"} {
		if bytes.Contains(data, []byte(message)) {
			select {
			case w.events <- message:
			default:
			}
		}
	}
	return len(data), nil
}

func TestCognitoCredentialCleanupRunsInConsoleAndPreservesLocalLogin(t *testing.T) {
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
			if _, err := db.Exec(`INSERT INTO identities (id, enabled) VALUES ('fixture-identity', 1);
INSERT INTO cognito_identities (identity_id, issuer, subject) VALUES ('fixture-identity', 'https://fixture.invalid', 'fixture-subject')`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO console_sessions
(id, identity_id, credential_version, token_hash, csrf_hash, created_at_ns, idle_expires_at_ns, absolute_expires_at_ns, auth_source)
VALUES ('fixture-session', 'fixture-identity', 1, ?, ?, ?, ?, ?, 'cognito')`,
				bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32), now.Add(-2*time.Hour).UnixNano(),
				now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO cognito_session_credentials
(session_id, identity_id, exchange_operation_id, original_nonce_hash, audience_json, groups_json,
issued_at_ns, token_expires_at_ns, claims_fresh_until_ns, refresh_generation, encrypted_refresh_token)
VALUES ('fixture-session', 'fixture-identity', 'fixture-exchange', ?, '["client"]', '[]', ?, ?, ?, 1, ?)`,
				bytes.Repeat([]byte{3}, 32), now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano(),
				now.Add(time.Minute).UnixNano(), bytes.Repeat([]byte{4}, 29)); err != nil {
				t.Fatal(err)
			}
			if mode == "audit failure" {
				if _, err := db.Exec(`CREATE TRIGGER reject_cleanup BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.credentials.cleanup'
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
			logger := slog.New(slog.NewTextHandler(cleanupSignalWriter{events: events}, nil))
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
					t.Fatalf("unexpected cleanup result: %s", event)
				}
			case err := <-done:
				finished = true
				t.Fatalf("Console stopped before cleanup: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("Console did not run its startup cleanup")
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
				t.Fatalf("cleanup %s blocked local login: status=%d", mode, response.StatusCode)
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
			var credentials, audits int
			var revoked sql.NullInt64
			if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = 'fixture-session'),
revoked_at_ns, (SELECT COUNT(*) FROM audit_events WHERE action = 'console.cognito.credentials.cleanup')
FROM console_sessions WHERE id = 'fixture-session'`).Scan(&credentials, &revoked, &audits); err != nil {
				t.Fatal(err)
			}
			if mode == "success" && (credentials != 0 || !revoked.Valid || audits != 1) {
				t.Fatal("startup cleanup did not persist its revocation and audit")
			}
			if mode == "audit failure" && (credentials != 1 || revoked.Valid || audits != 0) {
				t.Fatal("audit failure committed a partial startup cleanup")
			}
		})
	}
}
