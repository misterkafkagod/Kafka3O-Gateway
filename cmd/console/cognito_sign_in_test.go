package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type signInBrowser struct {
	f       *cognitoDiscoveryFixture
	cookies map[string]*http.Cookie
}

func newSignInBrowser(t *testing.T, publish bool) *signInBrowser {
	t.Helper()
	f := newCognitoDiscoveryFixture(t)
	if publish {
		services, err := newCognitoServices(context.Background(), f.db, f.config, rand.Reader, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.api.PublishCognito(services); err != nil {
			t.Fatal(err)
		}
	}
	return &signInBrowser{f: f, cookies: map[string]*http.Cookie{}}
}

// post sends a same-origin request with the browser's cookies and stores returned cookies.
func (b *signInBrowser) post(path, body string, sameOrigin bool, omit ...string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, cognitoTestOrigin+path, strings.NewReader(body))
	if sameOrigin {
		request.Header.Set("Origin", cognitoTestOrigin)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, cookie := range b.cookies {
		if !containsName(omit, name) {
			request.AddCookie(&http.Cookie{Name: name, Value: cookie.Value})
		}
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

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func (b *signInBrowser) start(t *testing.T) string {
	t.Helper()
	if response := b.post("/api/session/browser", "", true); response.Code != http.StatusNoContent {
		t.Fatalf("marker ensure status=%d", response.Code)
	}
	response := b.post("/api/session/cognito/start", "", true)
	var body struct {
		AuthorizationURL string `json:"authorizationUrl"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil || body.AuthorizationURL == "" {
		t.Fatalf("Cognito start status=%d", response.Code)
	}
	return body.AuthorizationURL
}

func callbackBody(query url.Values) string {
	data, _ := json.Marshal(struct {
		Query string `json:"query"`
	}{Query: query.Encode()})
	return string(data)
}

func problemCode(response *httptest.ResponseRecorder) string {
	var body struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	return body.Code
}

func TestCognitoSignInCompletesRoleFreeBoundSession(t *testing.T) {
	b := newSignInBrowser(t, true)
	authorizationURL := b.start(t)
	pre := b.cookies[cognito.BrowserAuthorizationCookieName]
	if pre == nil || !pre.Secure || !pre.HttpOnly || pre.SameSite != http.SameSiteLaxMode || pre.Path != "/" || pre.MaxAge <= 0 || pre.MaxAge > 600 {
		t.Fatal("pre-authentication cookie is missing or unsafe")
	}
	state, code := b.f.provider.authorize(t, authorizationURL, "kafka3o-admins", "unmapped-group")
	response := b.post("/api/session/cognito/callback", callbackBody(url.Values{"code": {code}, "state": {state}}), true)
	if response.Code != http.StatusOK {
		t.Fatalf("callback status=%d code=%s", response.Code, problemCode(response))
	}
	var principal struct {
		AccountID, AuthSource string
	}
	_ = json.Unmarshal(response.Body.Bytes(), &principal)
	if principal.AccountID == "" || principal.AuthSource != "cognito" {
		t.Fatal("callback did not return a Cognito session")
	}
	sessionCookie, csrfCookie := b.cookies[session.CookieName], b.cookies[session.CSRFCookieName]
	if sessionCookie == nil || csrfCookie == nil || sessionCookie.SameSite != http.SameSiteStrictMode || !sessionCookie.HttpOnly ||
		!sessionCookie.Secure || b.cookies[cognito.BrowserAuthorizationCookieName] != nil {
		t.Fatal("callback cookies are missing, unsafe or kept the pre-authentication cookie")
	}
	grants, err := b.f.db.ListAccountGrants(context.Background(), principal.AccountID)
	if err != nil || len(grants) != 0 {
		t.Fatalf("first Cognito login received grants: count=%d err=%v", len(grants), err)
	}
	if again := b.post("/api/session/cognito/start", "", true); again.Code != http.StatusConflict || problemCode(again) != "already_signed_in" {
		t.Fatalf("existing Cognito session was replaceable: status=%d", again.Code)
	}
	logs := b.f.logs.String()
	for _, secret := range []string{code, state, syntheticCognitoSecret, sessionCookie.Value, csrfCookie.Value,
		b.cookies[session.BrowserMarkerCookieName].Value, pre.Value} {
		if strings.Contains(logs, secret) {
			t.Fatal("sign-in logged a code, state, secret, marker or session credential")
		}
	}
}

func TestCognitoSignInRejectsReplayAndNeverRetriesTheProvider(t *testing.T) {
	b := newSignInBrowser(t, true)
	state, code := b.f.provider.authorize(t, b.start(t))
	pre := b.cookies[cognito.BrowserAuthorizationCookieName]
	body := callbackBody(url.Values{"code": {code}, "state": {state}})
	if first := b.post("/api/session/cognito/callback", body, true); first.Code != http.StatusOK {
		t.Fatalf("first callback status=%d", first.Code)
	}
	delete(b.cookies, session.CookieName)
	delete(b.cookies, session.CSRFCookieName)
	b.cookies[cognito.BrowserAuthorizationCookieName] = pre
	if replay := b.post("/api/session/cognito/callback", body, true); replay.Code != http.StatusUnauthorized || b.cookies[session.CookieName] != nil {
		t.Fatalf("replayed callback status=%d", replay.Code)
	}
	if calls := b.f.provider.tokenCalls.Load(); calls != 1 {
		t.Fatalf("token endpoint calls=%d, want 1", calls)
	}

	uncertain := newSignInBrowser(t, true)
	state, code = uncertain.f.provider.authorize(t, uncertain.start(t))
	uncertain.f.provider.tokenStatus.Store(http.StatusInternalServerError)
	pre = uncertain.cookies[cognito.BrowserAuthorizationCookieName]
	body = callbackBody(url.Values{"code": {code}, "state": {state}})
	if response := uncertain.post("/api/session/cognito/callback", body, true); response.Code != http.StatusUnauthorized || problemCode(response) != "sign_in_failed" {
		t.Fatalf("uncertain exchange status=%d", response.Code)
	}
	uncertain.f.provider.tokenStatus.Store(0)
	uncertain.cookies[cognito.BrowserAuthorizationCookieName] = pre
	if response := uncertain.post("/api/session/cognito/callback", body, true); response.Code != http.StatusUnauthorized || uncertain.cookies[session.CookieName] != nil {
		t.Fatalf("uncertain exchange was retried: status=%d", response.Code)
	}
	if calls := uncertain.f.provider.tokenCalls.Load(); calls != 1 {
		t.Fatalf("uncertain exchange token calls=%d, want 1", calls)
	}
}

func TestCognitoSignInRejectsForgedCallbacks(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(t *testing.T, b *signInBrowser, state, code string) (string, []string)
		status int
		code   string
	}{
		{"missing transient cookie", func(t *testing.T, b *signInBrowser, state, code string) (string, []string) {
			return callbackBody(url.Values{"code": {code}, "state": {state}}), []string{cognito.BrowserAuthorizationCookieName}
		}, http.StatusUnauthorized, "sign_in_failed"},
		{"missing marker", func(t *testing.T, b *signInBrowser, state, code string) (string, []string) {
			return callbackBody(url.Values{"code": {code}, "state": {state}}), []string{session.BrowserMarkerCookieName}
		}, http.StatusConflict, "invalid_browser_marker"},
		{"other browser marker", func(t *testing.T, b *signInBrowser, state, code string) (string, []string) {
			other, _, err := session.NewBrowserMarker(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			b.cookies[session.BrowserMarkerCookieName] = &http.Cookie{Name: session.BrowserMarkerCookieName, Value: other}
			return callbackBody(url.Values{"code": {code}, "state": {state}}), nil
		}, http.StatusUnauthorized, "sign_in_failed"},
		{"forged state", func(t *testing.T, b *signInBrowser, _, code string) (string, []string) {
			return callbackBody(url.Values{"code": {code}, "state": {strings.Repeat("A", 43)}}), nil
		}, http.StatusUnauthorized, "sign_in_failed"},
		{"unknown parameter", func(t *testing.T, b *signInBrowser, state, code string) (string, []string) {
			return callbackBody(url.Values{"code": {code}, "state": {state}, "next": {"https://attacker.example"}}), nil
		}, http.StatusBadRequest, "invalid_callback"},
		{"provider denial", func(t *testing.T, b *signInBrowser, state, _ string) (string, []string) {
			return callbackBody(url.Values{"error": {"access_denied"}, "state": {state}}), nil
		}, http.StatusUnauthorized, "sign_in_denied"},
		{"wrong nonce", func(t *testing.T, b *signInBrowser, state, code string) (string, []string) {
			b.f.provider.wrongNonce.Store(true)
			return callbackBody(url.Values{"code": {code}, "state": {state}}), nil
		}, http.StatusUnauthorized, "sign_in_failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newSignInBrowser(t, true)
			state, code := b.f.provider.authorize(t, b.start(t))
			body, omit := tt.mutate(t, b, state, code)
			response := b.post("/api/session/cognito/callback", body, true, omit...)
			if response.Code != tt.status || problemCode(response) != tt.code || b.cookies[session.CookieName] != nil {
				t.Fatalf("forged callback status=%d code=%s", response.Code, problemCode(response))
			}
			if strings.Contains(b.f.logs.String(), code) || strings.Contains(response.Body.String(), code) {
				t.Fatal("rejected callback exposed its authorization code")
			}
		})
	}
}

func TestCognitoSignInRequiresOriginMarkerAndAvailableProvider(t *testing.T) {
	b := newSignInBrowser(t, true)
	for _, path := range []string{"/api/session/cognito/start", "/api/session/cognito/callback"} {
		if response := b.post(path, `{"query":""}`, false); response.Code != http.StatusForbidden || len(response.Result().Cookies()) != 0 {
			t.Fatalf("%s without Origin status=%d", path, response.Code)
		}
	}
	if response := b.post("/api/session/cognito/start", "", true); response.Code != http.StatusConflict || problemCode(response) != "invalid_browser_marker" {
		t.Fatalf("start without marker status=%d", response.Code)
	}
	unavailable := newSignInBrowser(t, false)
	unavailable.post("/api/session/browser", "", true)
	if response := unavailable.post("/api/session/cognito/start", "", true); response.Code != http.StatusServiceUnavailable || problemCode(response) != "sso_unavailable" {
		t.Fatalf("start without discovery status=%d", response.Code)
	}
	if response := unavailable.post("/api/session/local", `{"username":"nobody","password":"not a real passphrase"}`, true); response.Code != http.StatusUnauthorized {
		t.Fatalf("local login during SSO outage status=%d", response.Code)
	}
}
