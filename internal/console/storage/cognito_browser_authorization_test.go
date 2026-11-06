package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestCognitoBrowserAuthorizationURLAndCookie(t *testing.T) {
	for _, authentication := range []cognito.ClientAuthentication{
		cognito.ClientAuthenticationPublic, cognito.ClientAuthenticationBasic,
	} {
		t.Run(string(authentication), func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", authentication)
			start, err := f.manager.StartAuthorization(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			location, err := f.exchanger.BrowserAuthorizationURL(start)
			if err != nil {
				t.Fatalf("construct authorization URL: %v", err)
			}
			parsed, err := url.Parse(location)
			if err != nil {
				t.Fatal("authorization URL was malformed")
			}
			endpoint, err := url.Parse(f.provider.AuthorizationURL)
			if err != nil || parsed.Scheme != endpoint.Scheme || parsed.Host != endpoint.Host ||
				parsed.Path != endpoint.Path || parsed.Fragment != "" || parsed.User != nil {
				t.Fatal("authorization URL did not use the exact verified endpoint")
			}
			want := map[string]string{
				"response_type": "code", "client_id": f.provider.ClientID, "redirect_uri": f.callback,
				"scope": "openid", "state": start.State, "nonce": start.Nonce,
				"code_challenge": start.CodeChallenge, "code_challenge_method": "S256",
			}
			query := parsed.Query()
			if len(query) != len(want) {
				t.Fatal("authorization URL contained missing or unexpected parameters")
			}
			for key, value := range want {
				if len(query[key]) != 1 || query.Get(key) != value {
					t.Fatalf("authorization parameter %s was incorrect", key)
				}
			}
			for _, secret := range []string{start.BrowserBinding, fixtureClientSecret, fixtureRefresh, fixtureAccess} {
				if strings.Contains(location, secret) {
					t.Fatal("authorization URL exposed a browser binding or provider credential")
				}
			}
			encoded, err := json.Marshal(start)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{start.State, start.Nonce, start.CodeChallenge, start.BrowserBinding} {
				if strings.Contains(string(encoded), secret) {
					t.Fatal("serialized authorization start exposed browser or provider values")
				}
			}
			cookie, err := cognito.BrowserAuthorizationCookie(start)
			if err != nil {
				t.Fatalf("construct browser authorization cookie: %v", err)
			}
			if cookie.Name != cognito.BrowserAuthorizationCookieName || cookie.Value != start.BrowserBinding ||
				cookie.Path != "/" || cookie.Domain != "" || !cookie.Secure || !cookie.HttpOnly ||
				cookie.SameSite != http.SameSiteLaxMode || !cookie.Expires.Equal(start.ExpiresAt) ||
				cookie.MaxAge != int(cognito.AttemptLifetime/time.Second) {
				t.Fatal("browser authorization cookie had unsafe attributes or lifetime")
			}
			wireCookie := cookie.String()
			for _, attribute := range []string{"Path=/", "Max-Age=300", "Secure", "HttpOnly", "SameSite=Lax"} {
				if !strings.Contains(wireCookie, attribute) {
					t.Fatal("serialized browser authorization cookie omitted a required attribute")
				}
			}
			if strings.Contains(wireCookie, "Domain=") {
				t.Fatal("serialized browser authorization cookie was not host-only")
			}
			cleared := cognito.ClearBrowserAuthorizationCookie()
			if cleared.Name != cookie.Name || cleared.Path != cookie.Path || cleared.Domain != "" ||
				cleared.Value != "" || cleared.MaxAge >= 0 || !cleared.Secure || !cleared.HttpOnly ||
				cleared.SameSite != http.SameSiteLaxMode || !cleared.Expires.Before(f.clock()) {
				t.Fatal("browser authorization cookie deletion did not match its secure scope")
			}
			if !strings.Contains(cleared.String(), "Max-Age=0") {
				t.Fatal("serialized cookie deletion did not expire the browser binding")
			}
			application, err := session.SessionCookie(start.BrowserBinding, start.ExpiresAt)
			if err != nil || application.SameSite != http.SameSiteStrictMode || !application.HttpOnly || !application.Secure {
				t.Fatal("application session cookie did not remain Strict, Secure and HttpOnly")
			}
			consumed, err := f.manager.ConsumeAuthorization(context.Background(), start.State, cookie.Value)
			if err != nil || strings.Contains(location, consumed.CodeVerifier) {
				t.Fatal("browser binding failed or the authorization URL exposed the PKCE verifier")
			}
			if f.tokenCalls.Load() != 0 {
				t.Fatal("preparing the browser request sent a provider token request")
			}
		})
	}
}

func TestCognitoBrowserAuthorizationRejectsModifiedAndForeignStarts(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	other, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		change func(*cognito.AuthorizationStart)
	}{
		{"operation", func(s *cognito.AuthorizationStart) { s.OperationID = other.OperationID }},
		{"state", func(s *cognito.AuthorizationStart) { s.State = other.State }},
		{"nonce", func(s *cognito.AuthorizationStart) { s.Nonce = other.Nonce }},
		{"challenge", func(s *cognito.AuthorizationStart) { s.CodeChallenge = other.CodeChallenge }},
		{"binding", func(s *cognito.AuthorizationStart) { s.BrowserBinding = other.BrowserBinding }},
		{"expiry", func(s *cognito.AuthorizationStart) { s.ExpiresAt = s.ExpiresAt.Add(time.Second) }},
		{"malformed", func(s *cognito.AuthorizationStart) { s.State = "invalid" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			modified := start
			tt.change(&modified)
			if location, err := f.exchanger.BrowserAuthorizationURL(modified); !errors.Is(err, cognito.ErrInvalidAttempt) || location != "" {
				t.Fatal("modified start produced an authorization URL")
			}
			if cookie, err := cognito.BrowserAuthorizationCookie(modified); !errors.Is(err, cognito.ErrInvalidAttempt) || cookie != nil {
				t.Fatal("modified start produced a browser cookie")
			}
		})
	}
	for _, callback := range []string{f.callback, f.callback + "/other"} {
		manager, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, callback)
		if err != nil {
			t.Fatal(err)
		}
		foreign, err := manager.StartAuthorization(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if location, err := f.exchanger.BrowserAuthorizationURL(foreign); !errors.Is(err, cognito.ErrInvalidAttempt) || location != "" {
			t.Fatal("another manager's attempt produced an authorization URL")
		}
		if callback != f.callback {
			if _, err := cognito.NewCodeExchanger(manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, ""); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
				t.Fatal("a different callback was accepted by the verified exchanger")
			}
		}
	}
	for _, change := range []func(*cognito.ProviderConfig){
		func(p *cognito.ProviderConfig) { p.Issuer += "/other" },
		func(p *cognito.ProviderConfig) { p.ClientID += "-other" },
		func(p *cognito.ProviderConfig) { p.AuthorizationURL += "/other" },
		func(p *cognito.ProviderConfig) { p.TokenURL += "/other" },
		func(p *cognito.ProviderConfig) { p.JWKSURL += "/other" },
	} {
		provider := f.provider
		change(&provider)
		manager, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, provider, f.callback)
		if err != nil {
			t.Fatal(err)
		}
		foreign, err := manager.StartAuthorization(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if location, err := f.exchanger.BrowserAuthorizationURL(foreign); !errors.Is(err, cognito.ErrInvalidAttempt) || location != "" {
			t.Fatal("another provider's attempt produced an authorization URL")
		}
		if _, err := cognito.NewCodeExchanger(manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, ""); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
			t.Fatal("a different provider was accepted by the verified exchanger")
		}
	}
	fabricated := cognito.AuthorizationStart{
		OperationID: start.OperationID, State: start.State, Nonce: start.Nonce,
		CodeChallenge: start.CodeChallenge, BrowserBinding: start.BrowserBinding, ExpiresAt: start.ExpiresAt,
	}
	if location, err := f.exchanger.BrowserAuthorizationURL(fabricated); !errors.Is(err, cognito.ErrInvalidAttempt) || location != "" {
		t.Fatal("fabricated start produced an authorization URL")
	}
	if cookie, err := cognito.BrowserAuthorizationCookie(fabricated); !errors.Is(err, cognito.ErrInvalidAttempt) || cookie != nil {
		t.Fatal("fabricated start produced a browser cookie")
	}
}

func TestCognitoBrowserAuthorizationExactExpiryAndClock(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	start, err := f.manager.StartAuthorization(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	created := f.clock()
	f.current.Store(start.ExpiresAt.Add(-time.Nanosecond).UnixNano())
	cookie, err := cognito.BrowserAuthorizationCookie(start)
	if err != nil || cookie.MaxAge != 1 {
		t.Fatal("live authorization was rejected before its deadline")
	}
	if _, err := f.exchanger.BrowserAuthorizationURL(start); err != nil {
		t.Fatal("live authorization URL was rejected before its deadline")
	}
	for _, now := range []time.Time{start.ExpiresAt, start.ExpiresAt.Add(time.Nanosecond), created.Add(-time.Nanosecond)} {
		f.current.Store(now.UnixNano())
		if location, err := f.exchanger.BrowserAuthorizationURL(start); !errors.Is(err, cognito.ErrInvalidAttempt) || location != "" {
			t.Fatal("expired or backwards-clock authorization produced a URL")
		}
		if cookie, err := cognito.BrowserAuthorizationCookie(start); !errors.Is(err, cognito.ErrInvalidAttempt) || cookie != nil {
			t.Fatal("expired or backwards-clock authorization produced a cookie")
		}
	}
}

func TestCognitoBrowserAuthorizationConcurrentPreparation(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	start, err := f.manager.StartAuthorization(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < cap(failures); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := f.exchanger.BrowserAuthorizationURL(start); err != nil {
				failures <- err
				return
			}
			if _, err := cognito.BrowserAuthorizationCookie(start); err != nil {
				failures <- err
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent preparation failed: %v", err)
	}
	if f.tokenCalls.Load() != 0 {
		t.Fatal("concurrent preparation dispatched a provider request")
	}
}
