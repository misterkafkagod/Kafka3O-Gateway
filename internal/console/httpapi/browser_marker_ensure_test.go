package httpapi_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type failingMarkerEntropy struct{}

func (failingMarkerEntropy) Read([]byte) (int, error) {
	return 0, errors.New("synthetic entropy failure")
}

func newBrowserMarkerAPI(t *testing.T, random io.Reader, logs io.Writer) *consoleapi.SessionAPI {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, nil))
	api, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		unusedAccountState{}, unusedAccountList{}, unusedAdminSessionList{}, unusedAdminSessionRevocation{}, failedSessions{},
		"https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	if random != nil {
		if err := api.EnableBrowserMarkers(random); err != nil {
			t.Fatal(err)
		}
	}
	return api
}

func ensureBrowserMarker(api http.Handler, target, origin string, cookies ...string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, target, nil)
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	for _, cookie := range cookies {
		request.Header.Add("Cookie", cookie)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func TestBrowserMarkerEnsureMintsOnlyWithoutMarker(t *testing.T) {
	var logs bytes.Buffer
	api := newBrowserMarkerAPI(t, rand.Reader, &logs)
	response := ensureBrowserMarker(api, "https://console.example/api/session/browser", "https://console.example")
	cookies := response.Result().Cookies()
	if response.Code != http.StatusNoContent || len(cookies) != 1 {
		t.Fatalf("first ensure status=%d cookies=%d", response.Code, len(cookies))
	}
	marker := cookies[0]
	if marker.Name != session.BrowserMarkerCookieName || marker.Path != "/" || marker.Domain != "" || !marker.Secure ||
		!marker.HttpOnly || marker.SameSite != http.SameSiteStrictMode || marker.MaxAge != 400*24*60*60 {
		t.Fatalf("marker cookie attributes are unsafe: %+v", *marker)
	}
	if _, err := session.HashBrowserMarker(marker.Value); err != nil {
		t.Fatal("minted marker is not canonical")
	}
	again := ensureBrowserMarker(api, "https://console.example/api/session/browser", "https://console.example",
		session.BrowserMarkerCookieName+"="+marker.Value)
	if again.Code != http.StatusNoContent || len(again.Result().Cookies()) != 0 || again.Header().Get("Set-Cookie") != "" {
		t.Fatalf("valid marker was replaced: status=%d", again.Code)
	}
	if strings.Contains(logs.String(), marker.Value) || strings.Contains(response.Body.String(), marker.Value) {
		t.Fatal("marker value was logged or returned in a body")
	}
}

func TestBrowserMarkerEnsureNeverOverwritesInvalidMarkers(t *testing.T) {
	valid, _, err := session.NewBrowserMarker(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	name := session.BrowserMarkerCookieName
	for _, tt := range []struct {
		name    string
		cookies []string
	}{
		{"malformed", []string{name + "=not-a-marker"}},
		{"empty", []string{name + "="}},
		{"missing value", []string{name}},
		{"noncanonical padding", []string{name + "=" + valid + "="}},
		{"duplicate in one header", []string{name + "=" + valid + "; " + name + "=" + valid}},
		{"duplicate across headers", []string{name + "=" + valid, name + "=" + valid}},
		{"malformed duplicate", []string{name + "=" + valid + "; " + name + "=x"}},
		{"spaced name", []string{" " + name + " =" + valid}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			api := newBrowserMarkerAPI(t, rand.Reader, &logs)
			response := ensureBrowserMarker(api, "https://console.example/api/session/browser", "https://console.example", tt.cookies...)
			if response.Code != http.StatusConflict || response.Header().Get("Set-Cookie") != "" {
				t.Fatalf("invalid marker status=%d set-cookie=%t", response.Code, response.Header().Get("Set-Cookie") != "")
			}
			if strings.Contains(logs.String(), valid) || strings.Contains(response.Body.String(), valid) {
				t.Fatal("invalid marker handling exposed marker material")
			}
		})
	}
}

func TestBrowserMarkerEnsureRequiresVerifiedOrigin(t *testing.T) {
	api := newBrowserMarkerAPI(t, rand.Reader, io.Discard)
	for _, tt := range []struct {
		name, target, origin string
	}{
		{"missing origin", "https://console.example/api/session/browser", ""},
		{"cross origin", "https://console.example/api/session/browser", "https://attacker.example"},
		{"plain HTTP", "http://console.example/api/session/browser", "https://console.example"},
		{"wrong host", "https://other.example/api/session/browser", "https://console.example"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := ensureBrowserMarker(api, tt.target, tt.origin)
			if response.Code != http.StatusForbidden || response.Header().Get("Set-Cookie") != "" {
				t.Fatalf("unverified ensure status=%d", response.Code)
			}
		})
	}
}

func TestBrowserMarkerEnsureFailsClosedOnEntropyFailure(t *testing.T) {
	var logs bytes.Buffer
	api := newBrowserMarkerAPI(t, failingMarkerEntropy{}, &logs)
	response := ensureBrowserMarker(api, "https://console.example/api/session/browser", "https://console.example")
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Set-Cookie") != "" {
		t.Fatalf("entropy failure status=%d", response.Code)
	}
	if strings.Contains(logs.String(), "synthetic entropy failure") || !strings.Contains(logs.String(), "error_type") {
		t.Fatal("entropy failure was not logged safely")
	}
}

func TestBrowserMarkerEnsureIsAbsentWithoutSSO(t *testing.T) {
	api := newBrowserMarkerAPI(t, nil, io.Discard)
	response := ensureBrowserMarker(api, "https://console.example/api/session/browser", "https://console.example")
	if response.Code != http.StatusNotFound || response.Header().Get("Set-Cookie") != "" {
		t.Fatalf("ensure route without SSO status=%d", response.Code)
	}
	if err := api.EnableBrowserMarkers(nil); err == nil {
		t.Fatal("browser markers were enabled without entropy")
	}
	if err := api.EnableBrowserMarkers(rand.Reader); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableBrowserMarkers(rand.Reader); err == nil {
		t.Fatal("browser markers were enabled twice")
	}
}
