package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func TestCognitoBrowserCallbackParsesOnlyExpectedParameters(t *testing.T) {
	state := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	code := "synthetic+opaque/code=with spaces"
	successQuery := url.Values{"state": {state}, "code": {code}}.Encode()
	callback, err := cognito.ParseBrowserCallback(successQuery)
	if err != nil || callback.State != state || callback.Code != code || callback.Denied {
		t.Fatal("valid authorization callback was not preserved")
	}
	deniedQuery := url.Values{
		"state": {state}, "error": {"access_denied"},
		"error_description": {"synthetic sensitive provider description"},
		"error_uri":         {"https://untrusted.example/synthetic-sensitive-location"},
	}.Encode()
	denied, err := cognito.ParseBrowserCallback(deniedQuery)
	if err != nil || denied.State != state || denied.Code != "" || !denied.Denied {
		t.Fatal("valid provider denial was not represented safely")
	}
	maxCode := strings.Repeat("a", 2048)
	maximum, err := cognito.ParseBrowserCallback(url.Values{"state": {state}, "code": {maxCode}}.Encode())
	if err != nil || maximum.Code != maxCode {
		t.Fatal("authorization code at the documented size limit was rejected")
	}
	maxQueryPrefix := "state=" + state + "&error=" + strings.Repeat("e", 128) +
		"&error_description=" + strings.Repeat("%21", 2048) + "&error_uri="
	maxQuery := maxQueryPrefix + strings.Repeat("a", 8192-len(maxQueryPrefix))
	if parsed, err := cognito.ParseBrowserCallback(maxQuery); err != nil || !parsed.Denied {
		t.Fatal("provider denial at the raw-query size limit was rejected")
	}
	if _, err := cognito.ParseBrowserCallback(maxQuery + "a"); !errors.Is(err, cognito.ErrInvalidBrowserCallback) {
		t.Fatal("provider denial above the raw-query size limit was accepted")
	}
	for _, result := range []cognito.BrowserCallback{callback, denied} {
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{state, code, "sensitive", "untrusted.example", "access_denied"} {
			if strings.Contains(string(encoded), secret) {
				t.Fatal("serialized callback exposed state, code or provider error details")
			}
		}
	}
	valid := "state=" + state
	for _, tt := range []struct {
		name  string
		query string
	}{
		{"empty query", ""},
		{"oversized query", strings.Repeat("a", 8193)},
		{"malformed encoding", valid + "&code=%"},
		{"semicolon", valid + "&code=abc;extra=1"},
		{"missing state", "code=abc"},
		{"duplicate state", valid + "&" + valid + "&code=abc"},
		{"invalid state", "state=invalid&code=abc"},
		{"missing outcome", valid},
		{"both outcomes", valid + "&code=abc&error=access_denied"},
		{"duplicate code", valid + "&code=abc&code=def"},
		{"empty code", valid + "&code="},
		{"oversized code", valid + "&code=" + strings.Repeat("a", 2049)},
		{"control in code", valid + "&code=%0A"},
		{"invalid UTF-8 code", valid + "&code=%FF"},
		{"description with code", valid + "&code=abc&error_description=oops"},
		{"error URI with code", valid + "&code=abc&error_uri=https%3A%2F%2Fexample.invalid"},
		{"unknown parameter", valid + "&code=abc&redirect_uri=https%3A%2F%2Fexample.invalid"},
		{"empty error", valid + "&error="},
		{"duplicate error", valid + "&error=access_denied&error=other"},
		{"oversized error", valid + "&error=" + strings.Repeat("a", 129)},
		{"control in description", valid + "&error=access_denied&error_description=%0D"},
		{"invalid UTF-8 description", valid + "&error=access_denied&error_description=%FF"},
		{"duplicate description", valid + "&error=access_denied&error_description=a&error_description=b"},
		{"oversized error URI", valid + "&error=access_denied&error_uri=" + strings.Repeat("a", 2049)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cognito.ParseBrowserCallback(tt.query)
			if !errors.Is(err, cognito.ErrInvalidBrowserCallback) || result != (cognito.BrowserCallback{}) {
				t.Fatal("unsafe callback was not rejected without data")
			}
			if strings.Contains(err.Error(), state) || strings.Contains(err.Error(), code) {
				t.Fatal("callback error exposed request values")
			}
		})
	}
}

func TestCognitoBrowserCallbackRequiresOneCanonicalBindingCookie(t *testing.T) {
	binding := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	name := cognito.BrowserAuthorizationCookieName
	for _, tt := range []struct {
		name    string
		headers []string
		valid   bool
	}{
		{"missing", nil, false},
		{"unrelated", []string{"other=value"}, false},
		{"empty", []string{name + "="}, false},
		{"malformed", []string{name + "=invalid"}, false},
		{"noncanonical", []string{name + "=" + binding[:42] + "B"}, false},
		{"duplicate", []string{name + "=" + binding + "; " + name + "=" + binding}, false},
		{"multiple headers", []string{name + "=" + binding, name + "=" + binding}, false},
		{"valid", []string{"other=value; " + name + "=" + binding}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "https://console.example/callback", nil)
			for _, value := range tt.headers {
				request.Header.Add("Cookie", value)
			}
			got, err := cognito.ReadBrowserAuthorizationCookie(request)
			if tt.valid {
				if err != nil || got != binding {
					t.Fatal("valid browser-binding cookie was rejected")
				}
			} else if !errors.Is(err, cognito.ErrInvalidBrowserCallback) || got != "" {
				t.Fatal("missing or ambiguous browser-binding cookie was accepted")
			} else if strings.Contains(err.Error(), binding) {
				t.Fatal("cookie error exposed the browser binding")
			}
		})
	}
	if binding, err := cognito.ReadBrowserAuthorizationCookie(nil); !errors.Is(err, cognito.ErrInvalidBrowserCallback) || binding != "" {
		t.Fatal("nil callback request was accepted")
	}
}

func TestCognitoBrowserCallbackConsumesAndExchangesOnlyOnce(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.nonce, f.challenge = start.Nonce, start.CodeChallenge
	f.mu.Unlock()
	raw := url.Values{"state": {start.State}, "code": {fixtureCode}}.Encode()
	callback, err := cognito.ParseBrowserCallback(raw)
	if err != nil || callback.Denied {
		t.Fatal("fixture callback was rejected")
	}
	other, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, callback.State, other.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatal("callback accepted another browser's binding")
	}
	cookie, err := cognito.BrowserAuthorizationCookie(start)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://console.example/callback", nil)
	request.AddCookie(cookie)
	binding, err := cognito.ReadBrowserAuthorizationCookie(request)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := f.manager.ConsumeAuthorization(ctx, callback.State, binding)
	if err != nil {
		t.Fatalf("consume parsed callback: %v", err)
	}
	exchange, err := f.exchanger.ExchangeAuthorizationCode(ctx, attempt, callback.Code)
	if err != nil || exchange.Identity.Subject != "opaque|synthetic-subject" || f.tokenCalls.Load() != 1 {
		t.Fatal("parsed callback did not complete the verified one-shot exchange")
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, callback.State, binding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatal("replayed callback was accepted")
	}
	if _, err := f.exchanger.ExchangeAuthorizationCode(ctx, attempt, callback.Code); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 1 {
		t.Fatal("replayed callback dispatched a second provider request")
	}
}

func TestCognitoBrowserCallbackCodeValidationAlsoProtectsExchange(t *testing.T) {
	for _, tt := range []struct {
		name string
		code string
	}{
		{"empty", ""},
		{"oversized", strings.Repeat("x", 2049)},
		{"control", "synthetic\ncode"},
		{"invalid UTF-8", string([]byte{0xff})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			_, consumed := f.consumeAttempt(t)
			result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, tt.code)
			if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || result.RefreshToken != "" || f.tokenCalls.Load() != 0 {
				t.Fatal("invalid authorization code reached the provider or returned credentials")
			}
			if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatal("rejected authorization code could be retried")
			}
		})
	}
}

func TestCognitoBrowserCallbackDenialConsumesWithoutExchange(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	callback, err := cognito.ParseBrowserCallback(url.Values{
		"state": {start.State}, "error": {"access_denied"}, "error_description": {"sensitive"},
	}.Encode())
	if err != nil || !callback.Denied || callback.Code != "" {
		t.Fatal("provider denial was not parsed")
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, callback.State, start.BrowserBinding); err != nil {
		t.Fatalf("consume provider denial: %v", err)
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, callback.State, start.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatal("provider denial was replayable")
	}
	if f.tokenCalls.Load() != 0 {
		t.Fatal("provider denial caused a token exchange")
	}
}
