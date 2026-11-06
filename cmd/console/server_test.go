package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func serverCertificate(t *testing.T, address string, notBefore, notAfter time.Time, usage x509.ExtKeyUsage) (string, string, *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Synthetic Console test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: notBefore, NotAfter: notAfter,
		IPAddresses: []net.IP{net.ParseIP(address)}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "server.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return certPath, keyPath, roots
}

func serverEnvironment(path, listen, origin, certificate, key string) func(string) string {
	values := map[string]string{
		"KAFKA3O_CONSOLE_DB_PATH":       path,
		"KAFKA3O_CONSOLE_LISTEN":        listen,
		"KAFKA3O_CONSOLE_ORIGIN":        origin,
		"KAFKA3O_CONSOLE_TLS_CERT_FILE": certificate,
		"KAFKA3O_CONSOLE_TLS_KEY_FILE":  key,
	}
	return func(name string) string { return values[name] }
}

func TestServerConfigRequiresLoopbackAndExactHTTPSOrigin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absolute")
	valid := serverEnvironment(path, "", "https://127.0.0.1:8443", path, path)
	config, err := loadServerConfig(valid)
	if err != nil || config.listen != "127.0.0.1:8443" {
		t.Fatalf("default configuration: %+v %v", config, err)
	}
	for _, tt := range []struct {
		listen, origin, database, certificate, key string
	}{
		{"0.0.0.0:8443", "https://127.0.0.1:8443", path, path, path},
		{"192.0.2.1:8443", "https://127.0.0.1:8443", path, path, path},
		{"localhost:8443", "https://localhost:8443", path, path, path},
		{"127.0.0.1:0", "https://127.0.0.1:8443", path, path, path},
		{"127.0.0.1:65536", "https://127.0.0.1:8443", path, path, path},
		{"127.0.0.1:8443", "http://127.0.0.1:8443", path, path, path},
		{"127.0.0.1:8443", "https://127.0.0.1:8443/", path, path, path},
		{"127.0.0.1:8443", "https://127.0.0.1:8443", "relative", path, path},
		{"127.0.0.1:8443", "https://127.0.0.1:8443", path, "relative", path},
		{"127.0.0.1:8443", "https://127.0.0.1:8443", path, path, "relative"},
	} {
		_, err := loadServerConfig(serverEnvironment(tt.database, tt.listen, tt.origin, tt.certificate, tt.key))
		if !errors.Is(err, errInvalidServerConfig) {
			t.Fatalf("invalid server configuration was accepted: %v", err)
		}
	}
	if err := run(context.Background(), []string{"serve"}, nil, func(string) string { return "present" }); !errors.Is(err, errBootstrapSecretSet) {
		t.Fatalf("server accepted a bootstrap secret in its environment: %v", err)
	}
}

func TestServerTLSRejectsInvalidCertificateAndKey(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name                string
		address             string
		notBefore, notAfter time.Time
		usage               x509.ExtKeyUsage
	}{
		{"hostname", "127.0.0.2", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth},
		{"expired", "127.0.0.1", now.Add(-2 * time.Hour), now.Add(-time.Hour), x509.ExtKeyUsageServerAuth},
		{"not yet valid", "127.0.0.1", now.Add(time.Hour), now.Add(2 * time.Hour), x509.ExtKeyUsageServerAuth},
		{"client only", "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageClientAuth},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cert, key, _ := serverCertificate(t, tt.address, tt.notBefore, tt.notAfter, tt.usage)
			_, err := loadServerTLS(serverConfig{origin: "https://127.0.0.1:8443", certificate: cert, key: key})
			if !errors.Is(err, errInvalidServerConfig) {
				t.Fatalf("invalid certificate was accepted: %v", err)
			}
		})
	}
	if runtime.GOOS != "windows" {
		cert, key, _ := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
		if err := os.Chmod(key, 0644); err != nil {
			t.Fatal(err)
		}
		_, err := loadServerTLS(serverConfig{origin: "https://127.0.0.1:8443", certificate: cert, key: key})
		if !errors.Is(err, errInvalidServerConfig) {
			t.Fatalf("insecure private key was accepted: %v", err)
		}
	}
}

func TestServerRequiresBootstrapBeforeOpeningListener(t *testing.T) {
	now := time.Now()
	cert, key, _ := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	path := filepath.Join(t.TempDir(), "console.sqlite")
	env := serverEnvironment(path, "127.0.0.1:8443", "https://127.0.0.1:8443", cert, key)
	listened := false
	listen := func(string, string) (net.Listener, error) {
		listened = true
		return nil, errors.New("unexpected listener")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runServe(context.Background(), env, listen, logger)
	if !errors.Is(err, storage.ErrBootstrapRequired) || listened {
		t.Fatalf("unbootstrapped server: listened=%t err=%v", listened, err)
	}
}

func TestTLSSessionServerEndToEnd(t *testing.T) {
	now := time.Now()
	cert, key, roots := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	path := filepath.Join(t.TempDir(), "console.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	const initialPassword, replacement = "initial passphrase", "replacement passphrase"
	const resetTemporary, finalPassword = "reset temporary passphrase", "final replacement passphrase"
	const userTemporary, userReplacement = "user temporary passphrase", "user replacement passphrase"
	if err := runBootstrap(context.Background(), path, secret, strings.NewReader(secret+"\nadmin\n"+initialPassword)); err != nil {
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
	ready := make(chan struct{})
	done := make(chan error, 1)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	go func() {
		done <- runServe(ctx, env, func(network, requested string) (net.Listener, error) {
			if network != "tcp" || requested != address {
				return nil, errors.New("unexpected listen address")
			}
			close(ready)
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
	case <-ready:
	case err := <-done:
		finished = true
		t.Fatalf("session server did not start: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("session server did not become ready")
	}
	if db, err := storage.OpenDatabase(context.Background(), path); db != nil || !errors.Is(err, storage.ErrDatabaseInUse) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("second Console store owner: %v", err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := func(method, route, body, requestOrigin string, cookies []*http.Cookie, csrf string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, origin+route, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if requestOrigin != "" {
			req.Header.Set("Origin", requestOrigin)
		}
		if csrf != "" {
			req.Header.Set(session.CSRFHeader, csrf)
		}
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp, data
	}
	resp, _ := request(http.MethodGet, "/api/session", "", "", nil, "")
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("anonymous session status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodPost, "/api/session/local", `{"username":"admin","password":"initial passphrase"}`, "https://attacker.example", nil, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin login status=%d", resp.StatusCode)
	}
	resp, data := request(http.MethodPost, "/api/session/local", `{"username":"admin","password":"initial passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":true`)) {
		t.Fatalf("bootstrap login status=%d", resp.StatusCode)
	}
	cookies := resp.Cookies()
	var csrf, rawSession string
	for _, cookie := range cookies {
		switch cookie.Name {
		case session.CSRFCookieName:
			csrf = cookie.Value
		case session.CookieName:
			rawSession = cookie.Value
		}
	}
	if csrf == "" || rawSession == "" {
		t.Fatal("login did not issue both session cookies")
	}
	resp, _ = request(http.MethodGet, "/api/v1/topics", "", "", cookies, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("session-only server exposed a Kafka route: %d", resp.StatusCode)
	}
	direct, err := http.NewRequest(http.MethodPost, origin+"/api/v1/topics", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	const forgedBearer = "synthetic-shared-bearer"
	direct.Header.Set("Authorization", "Bearer "+forgedBearer)
	resp, err = client.Do(direct)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("shared token reached an unknown Kafka route: %d", resp.StatusCode)
	}
	wrongHost, err := http.NewRequest(http.MethodGet, origin+"/api/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	wrongHost.Host = "attacker.invalid"
	resp, err = client.Do(wrongHost)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected Host status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, origin, cookies, csrf)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("password replacement status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodGet, "/api/session", "", "", cookies, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old session survived password replacement: %d", resp.StatusCode)
	}
	resp, data = request(http.MethodPost, "/api/session/local", `{"username":"admin","password":"replacement passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":false`)) {
		t.Fatalf("replacement login status=%d", resp.StatusCode)
	}
	var login struct {
		AccountID string `json:"accountId"`
	}
	if err := json.Unmarshal(data, &login); err != nil || login.AccountID == "" {
		t.Fatal("replacement login did not return the account ID")
	}
	cookies = resp.Cookies()
	csrf = ""
	for _, cookie := range cookies {
		if cookie.Name == session.CSRFCookieName {
			csrf = cookie.Value
		}
	}
	resetPath := "/api/admin/local-accounts/" + login.AccountID + "/password-reset"
	resetBody := `{"temporaryPassword":"reset temporary passphrase"}`
	resp, _ = request(http.MethodPost, resetPath, resetBody, origin, cookies, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("password reset without CSRF status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodPost, resetPath, resetBody, origin, cookies, csrf)
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"signInRequired":true`)) ||
		!bytes.Contains(data, []byte(`"operationId":`)) {
		t.Fatalf("administrator self-reset status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodGet, "/api/session", "", "", cookies, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("self-reset session remained valid: %d", resp.StatusCode)
	}
	resp, data = request(http.MethodPost, "/api/session/local", `{"username":"admin","password":"reset temporary passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":true`)) {
		t.Fatalf("reset login status=%d", resp.StatusCode)
	}
	cookies, csrf = resp.Cookies(), ""
	for _, cookie := range cookies {
		if cookie.Name == session.CSRFCookieName {
			csrf = cookie.Value
		}
	}
	resp, _ = request(http.MethodPost, "/api/session/password",
		`{"currentPassword":"reset temporary passphrase","newPassword":"final replacement passphrase"}`, origin, cookies, csrf)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset temporary password replacement status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodPost, "/api/session/local", `{"username":"admin","password":"final replacement passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":false`)) {
		t.Fatalf("final administrator login status=%d", resp.StatusCode)
	}
	cookies, csrf = resp.Cookies(), ""
	for _, cookie := range cookies {
		if cookie.Name == session.CSRFCookieName {
			csrf = cookie.Value
		}
	}
	resp, data = request(http.MethodPost, "/api/admin/local-accounts",
		`{"username":"Example-User","temporaryPassword":"user temporary passphrase"}`, origin, cookies, csrf)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("local account creation status=%d", resp.StatusCode)
	}
	var created struct {
		OperationID string `json:"operationId"`
		Account     struct {
			ID                 string `json:"id"`
			Username           string `json:"username"`
			MustChangePassword bool   `json:"mustChangePassword"`
		} `json:"account"`
	}
	if err := json.Unmarshal(data, &created); err != nil || created.OperationID == "" || created.Account.ID == "" ||
		created.Account.Username != "example-user" || !created.Account.MustChangePassword {
		t.Fatal("account creation response was invalid")
	}
	adminCookies, adminCSRF := cookies, csrf
	resp, data = request(http.MethodPost, "/api/session/local",
		`{"username":"example-user","password":"user temporary passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":true`)) {
		t.Fatalf("created user login status=%d", resp.StatusCode)
	}
	userCookies, userCSRF := resp.Cookies(), ""
	for _, cookie := range userCookies {
		if cookie.Name == session.CSRFCookieName {
			userCSRF = cookie.Value
		}
	}
	resp, _ = request(http.MethodPost, "/api/session/password",
		`{"currentPassword":"user temporary passphrase","newPassword":"user replacement passphrase"}`, origin, userCookies, userCSRF)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("created user password replacement status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodPost, "/api/session/local",
		`{"username":"example-user","password":"user replacement passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"mustChangePassword":false`)) {
		t.Fatalf("created user final login status=%d", resp.StatusCode)
	}
	userCookies, userCSRF = resp.Cookies(), ""
	for _, cookie := range userCookies {
		if cookie.Name == session.CSRFCookieName {
			userCSRF = cookie.Value
		}
	}
	resp, data = request(http.MethodPost, "/api/admin/local-accounts",
		`{"username":"forbidden","temporaryPassword":"other temporary passphrase"}`, origin, userCookies, userCSRF)
	if resp.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("role-free user account creation status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodGet, "/api/admin/local-accounts", "", "", userCookies, "")
	if resp.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) ||
		bytes.Contains(data, []byte(`"accounts":`)) {
		t.Fatalf("role-free user account listing status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodGet, "/api/admin/local-accounts?limit=1", "", "", adminCookies, "")
	var firstPage struct {
		OperationID string `json:"operationId"`
		Accounts    []struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"accounts"`
		NextCursor string `json:"nextCursor"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(data, &firstPage) != nil || firstPage.OperationID == "" ||
		len(firstPage.Accounts) != 1 || firstPage.Accounts[0].ID != login.AccountID ||
		firstPage.Accounts[0].Username != "admin" || firstPage.NextCursor != login.AccountID {
		t.Fatalf("administrator first account page status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodGet, "/api/admin/local-accounts?limit=1&after="+firstPage.NextCursor, "", "", adminCookies, "")
	var secondPage struct {
		OperationID string `json:"operationId"`
		Accounts    []struct {
			ID       string `json:"id"`
			Username string `json:"username"`
		} `json:"accounts"`
		NextCursor string `json:"nextCursor"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(data, &secondPage) != nil || secondPage.OperationID == "" ||
		len(secondPage.Accounts) != 1 || secondPage.Accounts[0].ID != created.Account.ID ||
		secondPage.Accounts[0].Username != "example-user" || secondPage.NextCursor != "" {
		t.Fatalf("administrator second account page status=%d", resp.StatusCode)
	}
	adminStatePath := "/api/admin/local-accounts/" + login.AccountID + "/enabled"
	resp, data = request(http.MethodPut, adminStatePath, `{"enabled":false}`, origin, adminCookies, adminCSRF)
	if resp.StatusCode != http.StatusConflict || !bytes.Contains(data, []byte(`"code":"last_local_global_admin"`)) {
		t.Fatalf("last administrator disable status=%d", resp.StatusCode)
	}
	userStatePath := "/api/admin/local-accounts/" + created.Account.ID + "/enabled"
	resp, data = request(http.MethodPut, userStatePath, `{"enabled":false}`, origin, userCookies, userCSRF)
	if resp.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("role-free user account disable status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodPut, userStatePath, `{"enabled":false}`, origin, adminCookies, adminCSRF)
	var disabled struct {
		OperationID    string `json:"operationId"`
		Enabled        bool   `json:"enabled"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(data, &disabled) != nil ||
		disabled.OperationID == "" || disabled.Enabled || disabled.SignInRequired {
		t.Fatalf("administrator account disable status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodGet, "/api/session", "", "", userCookies, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled user session remained valid: %d", resp.StatusCode)
	}
	resp, _ = request(http.MethodPost, "/api/session/local",
		`{"username":"example-user","password":"user replacement passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled user login status=%d", resp.StatusCode)
	}
	resp, data = request(http.MethodPut, userStatePath, `{"enabled":true}`, origin, adminCookies, adminCSRF)
	var enabled struct {
		OperationID string `json:"operationId"`
		Enabled     bool   `json:"enabled"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(data, &enabled) != nil ||
		enabled.OperationID == "" || !enabled.Enabled || enabled.OperationID == disabled.OperationID {
		t.Fatalf("administrator account re-enable status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodGet, "/api/session", "", "", userCookies, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("re-enabling revived a user session: %d", resp.StatusCode)
	}
	resp, _ = request(http.MethodPost, "/api/session/local",
		`{"username":"example-user","password":"user replacement passphrase"}`, origin, nil, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("re-enabled user login status=%d", resp.StatusCode)
	}
	cookies, csrf = adminCookies, adminCSRF
	resp, _ = request(http.MethodPost, "/api/session/logout", "", origin, cookies, csrf)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status=%d", resp.StatusCode)
	}
	resp, _ = request(http.MethodGet, "/api/session", "", "", cookies, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("logged-out session survived: %d", resp.StatusCode)
	}
	untrustedClient := &http.Client{Timeout: 5 * time.Second}
	if response, err := untrustedClient.Get(origin + "/api/session"); err == nil {
		response.Body.Close()
		t.Fatal("untrusted TLS certificate was accepted")
	}
	plainClient := &http.Client{Timeout: 5 * time.Second}
	plainResponse, err := plainClient.Get("http://" + address + "/api/session")
	if err != nil {
		t.Fatal(err)
	}
	plainResponse.Body.Close()
	if plainResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("plaintext HTTP status=%d", plainResponse.StatusCode)
	}
	oldTLS := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11}}
	defer oldTLS.CloseIdleConnections()
	if response, err := (&http.Client{Transport: oldTLS, Timeout: 5 * time.Second}).Get(origin + "/api/session"); err == nil {
		response.Body.Close()
		t.Fatal("TLS versions below 1.2 were accepted")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("server shutdown: %v", err)
	}
	finished = true
	db, err := storage.OpenDatabase(context.Background(), path)
	if err != nil {
		t.Fatalf("server did not release its database: %v", err)
	}
	if err := db.VerifyInitialAdministrator(context.Background()); err != nil {
		t.Fatalf("administrator was not preserved: %v", err)
	}
	stored, err := db.FindLocalAccount(context.Background(), "admin")
	if err != nil || stored.MustChangePassword {
		t.Fatalf("replacement password state was not preserved: %v", err)
	}
	matches, err := identity.VerifyLocalPassword(finalPassword, stored.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("final password was not persisted: %v", err)
	}
	events, err := db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != storage.AuditSucceeded || events[1].ResourceID != login.AccountID {
		t.Fatalf("administrator reset audit count=%d err=%v", len(events), err)
	}
	user, err := db.FindLocalAccount(context.Background(), "example-user")
	if err != nil || user.ID != created.Account.ID || user.MustChangePassword {
		t.Fatalf("created account was not preserved: %v", err)
	}
	matches, err = identity.VerifyLocalPassword(userReplacement, user.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("created user's replacement password was not persisted: %v", err)
	}
	grants, err := db.ListAccountGrants(context.Background(), created.Account.ID)
	if err != nil || len(grants) != 0 {
		t.Fatalf("created account gained implicit roles: count=%d err=%v", len(grants), err)
	}
	creationEvents, err := db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(creationEvents) != 3 || creationEvents[1].Outcome != storage.AuditSucceeded ||
		creationEvents[1].OperationID != created.OperationID || creationEvents[1].ResourceID != created.Account.ID ||
		creationEvents[2].Outcome != storage.AuditDenied {
		t.Fatalf("account creation audit count=%d err=%v", len(creationEvents), err)
	}
	listEvents, err := db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(listEvents) != 3 || listEvents[0].Outcome != storage.AuditDenied ||
		listEvents[1].OperationID != firstPage.OperationID || listEvents[2].OperationID != secondPage.OperationID ||
		listEvents[1].Outcome != storage.AuditSucceeded || listEvents[2].Outcome != storage.AuditSucceeded {
		t.Fatalf("account listing audit count=%d err=%v", len(listEvents), err)
	}
	disableEvents, err := db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(disableEvents) != 6 || disableEvents[5].OperationID != disabled.OperationID ||
		disableEvents[5].Outcome != storage.AuditSucceeded || disableEvents[5].ResourceID != created.Account.ID {
		t.Fatalf("account disable audit count=%d err=%v", len(disableEvents), err)
	}
	enableEvents, err := db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.enable", Limit: 10})
	if err != nil || len(enableEvents) != 2 || enableEvents[1].OperationID != enabled.OperationID ||
		enableEvents[1].Outcome != storage.AuditSucceeded || enableEvents[1].ResourceID != created.Account.ID {
		t.Fatalf("account enable audit count=%d err=%v", len(enableEvents), err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{secret, initialPassword, replacement, resetTemporary, finalPassword, userTemporary, userReplacement, rawSession, csrf, userCSRF, forgedBearer} {
		if strings.Contains(logs.String(), value) {
			t.Fatal("session server logged a credential")
		}
	}
}

type failingListener struct {
	err error
}

func (l failingListener) Accept() (net.Conn, error) { return nil, l.err }
func (l failingListener) Close() error              { return nil }
func (l failingListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8443}
}

func TestSessionServerSurfacesListenerFailure(t *testing.T) {
	now := time.Now()
	cert, key, _ := serverCertificate(t, "127.0.0.1", now.Add(-time.Hour), now.Add(time.Hour), x509.ExtKeyUsageServerAuth)
	path := filepath.Join(t.TempDir(), "console.sqlite")
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))
	if err := runBootstrap(context.Background(), path, secret, strings.NewReader(secret+"\nadmin\ninitial passphrase")); err != nil {
		t.Fatal(err)
	}
	env := serverEnvironment(path, "127.0.0.1:8443", "https://127.0.0.1:8443", cert, key)
	listenerErr := errors.New("listener failed")
	listen := func(string, string) (net.Listener, error) { return failingListener{err: listenerErr}, nil }
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := runServe(context.Background(), env, listen, logger); !errors.Is(err, listenerErr) {
		t.Fatalf("listener failure was not surfaced: %v", err)
	}
}
