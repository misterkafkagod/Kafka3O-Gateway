package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// call sends a same-origin request with the browser's cookies, adding the CSRF header for mutations.
func (b *signInBrowser) call(method, path, body string, csrf bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, cognitoTestOrigin+path, strings.NewReader(body))
	request.Header.Set("Origin", cognitoTestOrigin)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if cookie := b.cookies[session.CSRFCookieName]; csrf && cookie != nil {
		request.Header.Set(session.CSRFHeader, cookie.Value)
	}
	for name, cookie := range b.cookies {
		request.AddCookie(&http.Cookie{Name: name, Value: cookie.Value})
	}
	response := httptest.NewRecorder()
	b.f.api.ServeHTTP(response, request)
	for _, cookie := range response.Result().Cookies() {
		if cookie.MaxAge < 0 {
			delete(b.cookies, cookie.Name)
		} else {
			b.cookies[cookie.Name] = cookie
		}
	}
	return response
}

func cloneCookies(cookies map[string]*http.Cookie) map[string]*http.Cookie {
	clone := make(map[string]*http.Cookie, len(cookies))
	for name, cookie := range cookies {
		copied := *cookie
		clone[name] = &copied
	}
	return clone
}

func (b *signInBrowser) signIn(t *testing.T, groups ...string) {
	t.Helper()
	state, code := b.f.provider.authorize(t, b.start(t), groups...)
	if response := b.post("/api/session/cognito/callback", callbackBody(url.Values{"code": {code}, "state": {state}}), true); response.Code != http.StatusOK {
		t.Fatalf("Cognito sign-in status=%d code=%s", response.Code, problemCode(response))
	}
}

func (b *signInBrowser) source(t *testing.T) (int, string) {
	t.Helper()
	response := b.call(http.MethodGet, "/api/session", "", false)
	var body struct {
		AuthSource string `json:"authSource"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	return response.Code, body.AuthSource
}

func TestBoundCognitoSessionAPIReadsAndSignsOut(t *testing.T) {
	b := newSignInBrowser(t, true)
	b.signIn(t)
	if status, source := b.source(t); status != http.StatusOK || source != "cognito" {
		t.Fatalf("bound session read status=%d source=%s", status, source)
	}
	if response := b.call(http.MethodGet, "/api/admin/sessions", "", false); response.Code != http.StatusForbidden {
		t.Fatalf("role-free Cognito session reached administration: status=%d", response.Code)
	}
	stale := cloneCookies(b.cookies)
	if response := b.call(http.MethodPost, "/api/session/logout", "", false); response.Code != http.StatusForbidden {
		t.Fatalf("logout without CSRF status=%d", response.Code)
	}
	response := b.call(http.MethodPost, "/api/session/logout", "", true)
	var body struct {
		ProviderLogoutURL string `json:"providerLogoutUrl"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil ||
		!strings.HasPrefix(body.ProviderLogoutURL, b.f.provider.server.URL+"/logout?") || b.cookies[session.CookieName] != nil {
		t.Fatalf("Cognito logout status=%d did not revoke locally or return the provider logout URL", response.Code)
	}
	b.cookies = cloneCookies(stale)
	if status, _ := b.source(t); status != http.StatusUnauthorized {
		t.Fatalf("revoked Cognito session was usable: status=%d", status)
	}
	b.cookies = cloneCookies(stale)
	if response := b.call(http.MethodPost, "/api/session/logout", "", true); response.Code != http.StatusUnauthorized {
		t.Fatalf("replayed logout status=%d", response.Code)
	}
	if logs := b.f.logs.String(); strings.Contains(logs, stale[session.CookieName].Value) || strings.Contains(logs, stale[session.BrowserMarkerCookieName].Value) {
		t.Fatal("bound session handling logged a session token or marker")
	}
}

func TestBoundCognitoSessionAPIRequiresTheBindingMarker(t *testing.T) {
	b := newSignInBrowser(t, true)
	b.signIn(t)
	original := cloneCookies(b.cookies)
	other, _, err := session.NewBrowserMarker(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	b.cookies[session.BrowserMarkerCookieName] = &http.Cookie{Name: session.BrowserMarkerCookieName, Value: other}
	if status, _ := b.source(t); status != http.StatusUnauthorized {
		t.Fatalf("session was usable with another browser's marker: status=%d", status)
	}
	b.cookies = map[string]*http.Cookie{}
	for name, cookie := range original {
		if name != session.BrowserMarkerCookieName {
			b.cookies[name] = cookie
		}
	}
	if status, _ := b.source(t); status != http.StatusUnauthorized {
		t.Fatalf("session was usable without its marker: status=%d", status)
	}
	b.cookies = cloneCookies(original)
	if status, source := b.source(t); status != http.StatusOK || source != "cognito" {
		t.Fatalf("a wrong marker revoked the correct session: status=%d", status)
	}
	if response := b.call(http.MethodPost, "/api/session/logout", "", true); response.Code != http.StatusOK {
		t.Fatalf("logout status=%d", response.Code)
	}
	b.signIn(t)
	replacement := b.cookies[session.CookieName].Value
	b.cookies[session.CookieName] = original[session.CookieName]
	if status, _ := b.source(t); status != http.StatusUnauthorized {
		t.Fatalf("replaced session was usable: status=%d", status)
	}
	b.cookies[session.CookieName] = &http.Cookie{Name: session.CookieName, Value: replacement}
	if status, _ := b.source(t); status != http.StatusOK {
		t.Fatalf("replacement session was unusable: status=%d", status)
	}
}

func TestBoundCognitoSessionAPIRefreshesStaleClaimsAndFailsClosed(t *testing.T) {
	b := newSignInBrowser(t, true)
	// Tokens issued almost five minutes ago keep claims fresh for only a few seconds.
	b.f.provider.issuedAge.Store(int64(5*time.Minute - 4*time.Second))
	b.signIn(t, "kafka3o-admins")
	if status, _ := b.source(t); status != http.StatusOK || b.f.provider.tokenCalls.Load() != 1 {
		t.Fatalf("fresh claims status=%d calls=%d", status, b.f.provider.tokenCalls.Load())
	}
	time.Sleep(2500 * time.Millisecond)
	if status, _ := b.source(t); status != http.StatusOK {
		t.Fatalf("stale claims were not refreshed: status=%d", status)
	}
	if calls := b.f.provider.tokenCalls.Load(); calls != 2 {
		t.Fatalf("token calls after refresh=%d, want 2", calls)
	}
	time.Sleep(4500 * time.Millisecond)
	b.f.provider.tokenStatus.Store(http.StatusInternalServerError)
	for range 2 {
		if status, _ := b.source(t); status != http.StatusServiceUnavailable && status != http.StatusUnauthorized {
			t.Fatalf("failed refresh status=%d", status)
		}
	}
	if calls := b.f.provider.tokenCalls.Load(); calls != 3 {
		t.Fatalf("failed refresh was retried automatically: token calls=%d, want 3", calls)
	}
	b.f.provider.tokenStatus.Store(0)
	if status, _ := b.source(t); status == http.StatusOK {
		t.Fatal("an uncertain refresh was followed by an unverified session")
	}
}

func TestBoundCognitoSessionAPIKeepsLocalSessionsAndRejectsReplacement(t *testing.T) {
	b := newSignInBrowser(t, true)
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(b.f.db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(context.Background(), secret, "admin", "initial passphrase"); err != nil {
		t.Fatal(err)
	}
	b.post("/api/session/browser", "", true)
	const local = `{"username":"admin","password":"initial passphrase"}`
	if response := b.call(http.MethodPost, "/api/session/local", local, false); response.Code != http.StatusOK {
		t.Fatalf("local login with a marker status=%d", response.Code)
	}
	if status, source := b.source(t); status != http.StatusOK || source != "local" {
		t.Fatalf("local session read status=%d source=%s", status, source)
	}
	if response := b.call(http.MethodPost, "/api/session/cognito/start", "", false); response.Code != http.StatusConflict {
		t.Fatalf("SSO replaced a local session: status=%d", response.Code)
	}
	if response := b.call(http.MethodPost, "/api/session/logout", "", true); response.Code != http.StatusNoContent {
		t.Fatalf("local logout status=%d", response.Code)
	}
	b.signIn(t)
	if response := b.call(http.MethodPost, "/api/session/local", local, false); response.Code != http.StatusConflict || problemCode(response) != "already_signed_in" {
		t.Fatalf("local login replaced a bound Cognito session: status=%d", response.Code)
	}
	unavailable := newSignInBrowser(t, false)
	unavailable.post("/api/session/browser", "", true)
	unavailable.cookies[session.CookieName] = b.cookies[session.CookieName]
	if response := unavailable.call(http.MethodGet, "/api/session", "", false); response.Code != http.StatusServiceUnavailable || unavailable.cookies[session.CookieName] == nil {
		t.Fatalf("session before discovery was cleared instead of reported unavailable: status=%d", response.Code)
	}
}
