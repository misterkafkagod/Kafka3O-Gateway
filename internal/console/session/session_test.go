package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

type testStore struct{}

func (testStore) CreateLocalSession(context.Context, Record) (Principal, error) {
	return Principal{}, errors.New("unexpected store call")
}

func (testStore) UseSession(context.Context, [32]byte, func() time.Time, time.Duration, *cognito.RefreshTokenProtector) (Principal, error) {
	return Principal{}, errors.New("unexpected store call")
}

func (testStore) RevokeSession(context.Context, [32]byte, string, time.Time) error {
	return errors.New("unexpected store call")
}

type failedRandom struct{}

func (failedRandom) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestSessionConfigurationAndEntropyFailure(t *testing.T) {
	for _, tt := range []struct {
		store          Store
		random         io.Reader
		now            func() time.Time
		idle, absolute time.Duration
	}{
		{nil, rand.Reader, time.Now, time.Minute, time.Hour},
		{testStore{}, nil, time.Now, time.Minute, time.Hour},
		{testStore{}, rand.Reader, nil, time.Minute, time.Hour},
		{testStore{}, rand.Reader, time.Now, 0, time.Hour},
		{testStore{}, rand.Reader, time.Now, 2 * time.Hour, time.Hour},
		{testStore{}, rand.Reader, time.Now, time.Minute, MaxAbsoluteLifetime + time.Second},
	} {
		if _, err := NewManager(tt.store, tt.random, tt.now, tt.idle, tt.absolute); !errors.Is(err, ErrInvalidSessionConfig) {
			t.Errorf("invalid session configuration: %v", err)
		}
	}
	manager, err := NewManager(testStore{}, failedRandom{}, time.Now, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.IssueLocal(context.Background(), identity.LocalAccount{ID: "user", Enabled: true, CredentialVersion: 1})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("entropy failure: %v", err)
	}
}

func TestResolveRejectsInvalidClockBeforeUsingStore(t *testing.T) {
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	for _, invalid := range []time.Time{{}, time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)} {
		manager, err := NewManager(testStore{}, rand.Reader, func() time.Time { return invalid }, time.Minute, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Resolve(context.Background(), token); !errors.Is(err, ErrInvalidSessionConfig) {
			t.Fatalf("invalid resolve clock: %v", err)
		}
	}
}

func TestSessionCookiesAreHostOnlySecureAndClearable(t *testing.T) {
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	expires := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	cookie, err := SessionCookie(token, expires)
	if err != nil {
		t.Fatal(err)
	}
	if cookie.Name != CookieName || cookie.Path != "/" || cookie.Domain != "" || !cookie.Secure || !cookie.HttpOnly ||
		cookie.SameSite != http.SameSiteStrictMode || !cookie.Expires.Equal(expires) {
		t.Fatalf("invalid session cookie attributes: %+v", cookie)
	}
	csrfCookie, err := CSRFCookie(token, expires)
	if err != nil || csrfCookie.Name != CSRFCookieName || csrfCookie.Path != "/" || csrfCookie.Domain != "" ||
		!csrfCookie.Secure || csrfCookie.HttpOnly || csrfCookie.SameSite != http.SameSiteStrictMode {
		t.Fatal("invalid CSRF cookie attributes")
	}
	for _, cleared := range []*http.Cookie{ClearSessionCookie(), ClearCSRFCookie()} {
		if cleared.MaxAge >= 0 || !cleared.Secure || cleared.Domain != "" || cleared.Path != "/" {
			t.Fatal("cookie was not safely cleared")
		}
	}
	if _, err := SessionCookie("not-a-token", expires); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("malformed cookie token: %v", err)
	}
}

func TestSessionAccessRequiresPasswordReplacement(t *testing.T) {
	principal := Principal{SessionID: "session", AccountID: "user", MustChangePassword: true}
	if err := CheckSessionAccess(principal, AccessNormal); !errors.Is(err, ErrPasswordChangeRequired) {
		t.Fatalf("ordinary access with temporary password: %v", err)
	}
	for _, purpose := range []AccessPurpose{AccessPasswordChange, AccessLogout, AccessSessionInfo} {
		if err := CheckSessionAccess(principal, purpose); err != nil {
			t.Errorf("restricted purpose %s: %v", purpose, err)
		}
	}
	principal.MustChangePassword = false
	if err := CheckSessionAccess(principal, AccessNormal); err != nil {
		t.Fatal(err)
	}
	if err := CheckSessionAccess(principal, AccessPurpose("unknown")); !errors.Is(err, ErrInvalidAccessPurpose) {
		t.Fatalf("unknown purpose: %v", err)
	}
	if err := CheckSessionAccess(Principal{}, AccessLogout); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("empty principal: %v", err)
	}
}

func TestCSRFRequiresExactOriginAndSessionBoundHeader(t *testing.T) {
	verifier, err := NewCSRFVerifier("https://console.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, origin := range []string{"http://console.example", "https://console.example/path", "https://user@console.example", "*", ""} {
		if _, err := NewCSRFVerifier(origin); !errors.Is(err, ErrInvalidSessionConfig) {
			t.Errorf("invalid configured origin %q: %v", origin, err)
		}
	}
	token := base64.RawURLEncoding.EncodeToString([]byte("01234567890123456789012345678901"))
	hash, err := hashSessionToken(token)
	if err != nil {
		t.Fatal(err)
	}
	principal := Principal{SessionID: "session", AccountID: "user", CSRFHash: hash}
	request := httptest.NewRequest(http.MethodPost, "https://console.example/api", nil)
	request.Header.Set("Origin", "https://console.example")
	request.Header.Set(CSRFHeader, token)
	if err := verifier.Verify(request, principal); err != nil {
		t.Fatal(err)
	}
	for _, origin := range []string{"", "null", "https://console.example.evil", "http://console.example"} {
		request.Header.Set("Origin", origin)
		if err := verifier.Verify(request, principal); !errors.Is(err, ErrInvalidCSRF) {
			t.Errorf("untrusted origin %q: %v", origin, err)
		}
	}
	request.Header.Set("Origin", "https://console.example")
	request.Header.Add("Origin", "https://console.example")
	if err := verifier.VerifyOrigin(request); !errors.Is(err, ErrInvalidCSRF) {
		t.Fatalf("duplicate origin: %v", err)
	}
	request.Header.Set("Origin", "https://console.example")
	request.Header.Set(CSRFHeader, base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
	if err := verifier.Verify(request, principal); !errors.Is(err, ErrInvalidCSRF) {
		t.Fatalf("wrong CSRF token: %v", err)
	}
	request.Header.Set(CSRFHeader, token)
	request.Header.Add(CSRFHeader, token)
	if err := verifier.Verify(request, principal); !errors.Is(err, ErrInvalidCSRF) {
		t.Fatalf("duplicate CSRF token: %v", err)
	}
}

func TestSessionCredentialsAreNotSerialized(t *testing.T) {
	encoded, err := json.Marshal(Credentials{Token: "secret-session-token", CSRFToken: "secret-csrf-token"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret-") {
		t.Fatal("serialized credentials contain bearer or CSRF tokens")
	}
}
