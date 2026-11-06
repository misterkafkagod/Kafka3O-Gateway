package cognito

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCognitoVerifyRefreshedIDToken(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	key := newFixtureKey(t, "refresh-key")
	forged := newFixtureKey(t, key.id)
	fixture := newCognitoFixture(t, key)
	verifier, err := NewVerifier(context.Background(), fixture.config, fixture.roots, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	nonceHash := sha256.Sum256([]byte("synthetic-nonce"))
	originalClaims := cognitoFixtureClaims(fixture.config, now)
	originalClaims["aud"] = []string{fixture.config.ClientID, "another-client"}
	originalClaims["azp"] = fixture.config.ClientID
	previous, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, key, "RS256", originalClaims), nonceHash)
	if err != nil {
		t.Fatal(err)
	}
	refreshedClaims := func() map[string]any {
		claims := cognitoFixtureClaims(fixture.config, now)
		claims["aud"] = []string{"another-client", fixture.config.ClientID}
		claims["azp"] = fixture.config.ClientID
		claims["iat"] = now.Unix()
		claims["cognito:groups"] = []string{"newly-mapped-group"}
		delete(claims, "nonce")
		return claims
	}
	for _, tt := range []struct {
		name   string
		change func(map[string]any)
		groups []string
	}{
		{"nonce absent", func(map[string]any) {}, []string{"newly-mapped-group"}},
		{"original nonce present", func(c map[string]any) { c["nonce"] = "synthetic-nonce" }, []string{"newly-mapped-group"}},
		{"groups absent", func(c map[string]any) { delete(c, "cognito:groups") }, nil},
		{"groups empty", func(c map[string]any) { c["cognito:groups"] = []string{} }, []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claims := refreshedClaims()
			tt.change(claims)
			identity, err := verifier.VerifyRefreshedIDToken(
				context.Background(), signFixtureToken(t, key, "RS256", claims), previous, nonceHash,
			)
			if err != nil || identity.Issuer != previous.Issuer || identity.Subject != previous.Subject ||
				!reflect.DeepEqual(identity.Audience, []string{"another-client", fixture.config.ClientID}) ||
				!reflect.DeepEqual(identity.Groups, tt.groups) ||
				!identity.IssuedAt.Equal(now) || !identity.ExpiresAt.Equal(now.Add(time.Hour)) {
				t.Fatalf("valid refreshed identity was rejected or incomplete: %v", err)
			}
		})
	}
	for _, tt := range []struct {
		name   string
		change func(map[string]any)
		key    fixtureKey
	}{
		{"wrong issuer", func(c map[string]any) { c["iss"] = "https://other.invalid/pool" }, key},
		{"wrong subject", func(c map[string]any) { c["sub"] = "another-subject" }, key},
		{"different audience", func(c map[string]any) { c["aud"] = []string{fixture.config.ClientID, "other-client"} }, key},
		{"wrong client", func(c map[string]any) { c["aud"] = []string{"another-client", "other-client"} }, key},
		{"wrong authorized party", func(c map[string]any) { c["azp"] = "other-client" }, key},
		{"wrong nonce", func(c map[string]any) { c["nonce"] = "another-nonce" }, key},
		{"empty nonce", func(c map[string]any) { c["nonce"] = "" }, key},
		{"null nonce", func(c map[string]any) { c["nonce"] = nil }, key},
		{"wrong nonce type", func(c map[string]any) { c["nonce"] = 42 }, key},
		{"wrong token use", func(c map[string]any) { c["token_use"] = "access" }, key},
		{"expired", func(c map[string]any) { c["exp"] = now.Add(-time.Second).Unix() }, key},
		{"expiry boundary", func(c map[string]any) { c["exp"] = now.Unix() }, key},
		{"stale issuance", func(c map[string]any) { c["iat"] = previous.IssuedAt.Unix() }, key},
		{"older issuance", func(c map[string]any) { c["iat"] = previous.IssuedAt.Add(-time.Second).Unix() }, key},
		{"future issuance", func(c map[string]any) { c["iat"] = now.Add(6 * time.Minute).Unix() }, key},
		{"missing issuance", func(c map[string]any) { delete(c, "iat") }, key},
		{"wrong groups type", func(c map[string]any) { c["cognito:groups"] = "ADMIN" }, key},
		{"null groups", func(c map[string]any) { c["cognito:groups"] = nil }, key},
		{"null group member", func(c map[string]any) { c["cognito:groups"] = []any{"engineers", nil} }, key},
		{"empty group member", func(c map[string]any) { c["cognito:groups"] = []string{""} }, key},
		{"forged signature", func(map[string]any) {}, forged},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claims := refreshedClaims()
			tt.change(claims)
			raw := signFixtureToken(t, tt.key, "RS256", claims)
			identity, err := verifier.VerifyRefreshedIDToken(context.Background(), raw, previous, nonceHash)
			if !errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
				t.Fatalf("invalid refresh was accepted: %v", err)
			}
			if strings.Contains(err.Error(), raw) || strings.Contains(err.Error(), "synthetic-nonce") {
				t.Fatal("refresh verification error exposed credentials")
			}
		})
	}
	raw := signFixtureToken(t, key, "RS256", refreshedClaims())
	for _, tt := range []struct {
		name     string
		previous VerifiedIdentity
		nonce    [32]byte
	}{
		{"missing original", VerifiedIdentity{}, nonceHash},
		{"missing original nonce", previous, [32]byte{}},
		{"wrong previous issuer", VerifiedIdentity{Issuer: "https://other.invalid", Subject: previous.Subject, Audience: previous.Audience, IssuedAt: previous.IssuedAt}, nonceHash},
		{"missing previous issuance", VerifiedIdentity{Issuer: previous.Issuer, Subject: previous.Subject, Audience: previous.Audience}, nonceHash},
		{"missing previous client", VerifiedIdentity{Issuer: previous.Issuer, Subject: previous.Subject, Audience: []string{"another-client"}, IssuedAt: previous.IssuedAt}, nonceHash},
	} {
		t.Run(tt.name, func(t *testing.T) {
			identity, err := verifier.VerifyRefreshedIDToken(context.Background(), raw, tt.previous, tt.nonce)
			if !errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
				t.Fatalf("invalid previous identity was accepted: %v", err)
			}
		})
	}
	withNonce := refreshedClaims()
	withNonce["nonce"] = "synthetic-nonce"
	wrongNonceHash := sha256.Sum256([]byte("different-original-nonce"))
	if _, err := verifier.VerifyRefreshedIDToken(context.Background(), signFixtureToken(t, key, "RS256", withNonce), previous, wrongNonceHash); !errors.Is(err, ErrInvalidIDToken) {
		t.Fatalf("wrong retained nonce was accepted: %v", err)
	}
	if _, err := verifier.VerifyRefreshedIDToken(context.Background(), "", previous, nonceHash); !errors.Is(err, ErrInvalidIDToken) {
		t.Fatalf("missing ID token was accepted: %v", err)
	}
	oldPrevious := previous
	oldPrevious.IssuedAt = now.Add(-time.Hour)
	for _, tt := range []struct {
		name    string
		issued  time.Time
		wantErr bool
	}{
		{"fresh boundary", now.Add(-5*time.Minute + time.Second), false},
		{"stale boundary", now.Add(-5 * time.Minute), true},
		{"stale token", now.Add(-6 * time.Minute), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claims := refreshedClaims()
			claims["iat"] = tt.issued.Unix()
			identity, err := verifier.VerifyRefreshedIDToken(
				context.Background(), signFixtureToken(t, key, "RS256", claims), oldPrevious, nonceHash,
			)
			if (err != nil) != tt.wantErr || (tt.wantErr && (!errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}))) {
				t.Fatalf("refresh freshness boundary was not enforced: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := verifier.VerifyRefreshedIDToken(ctx, raw, previous, nonceHash); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled verification returned %v", err)
	}
}

func TestCognitoRefreshedIDTokenFailsClosedOnJWKSOutage(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	key := newFixtureKey(t, "original")
	rotated := newFixtureKey(t, "rotated")
	fixture := newCognitoFixture(t, key)
	verifier, err := NewVerifier(context.Background(), fixture.config, fixture.roots, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	nonceHash := sha256.Sum256([]byte("synthetic-nonce"))
	previous, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, key, "RS256", cognitoFixtureClaims(fixture.config, now)), nonceHash)
	if err != nil {
		t.Fatal(err)
	}
	claims := cognitoFixtureClaims(fixture.config, now)
	claims["iat"] = now.Unix()
	delete(claims, "nonce")
	fixture.mu.Lock()
	fixture.jwksStatus = http.StatusServiceUnavailable
	fixture.mu.Unlock()
	identity, err := verifier.VerifyRefreshedIDToken(context.Background(), signFixtureToken(t, rotated, "RS256", claims), previous, nonceHash)
	if !errors.Is(err, ErrProviderUnavailable) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
		t.Fatalf("JWKS outage did not fail closed: %v", err)
	}
	fixture.mu.Lock()
	fixture.jwksStatus = 0
	fixture.keys = []fixtureKey{rotated}
	fixture.mu.Unlock()
	identity, err = verifier.VerifyRefreshedIDToken(context.Background(), signFixtureToken(t, rotated, "RS256", claims), previous, nonceHash)
	if err != nil || identity.Subject != previous.Subject {
		t.Fatalf("valid refreshed identity failed after JWKS rotation: %v", err)
	}
}

func TestCognitoRefreshRechecksExpiryAfterVerification(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	key := newFixtureKey(t, "expiry-key")
	fixture := newCognitoFixture(t, key)
	verifier, err := NewVerifier(context.Background(), fixture.config, fixture.roots, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	nonceHash := sha256.Sum256([]byte("synthetic-nonce"))
	previous, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, key, "RS256", cognitoFixtureClaims(fixture.config, now)), nonceHash)
	if err != nil {
		t.Fatal(err)
	}
	claims := cognitoFixtureClaims(fixture.config, now)
	claims["iat"] = now.Unix()
	claims["exp"] = now.Add(time.Second).Unix()
	delete(claims, "nonce")
	calls := 0
	verifier.now = func() time.Time {
		calls++
		if calls == 1 {
			return now
		}
		return now.Add(time.Second)
	}
	identity, err := verifier.VerifyRefreshedIDToken(context.Background(), signFixtureToken(t, key, "RS256", claims), previous, nonceHash)
	if !errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
		t.Fatalf("token expiring during verification was accepted: %v", err)
	}
}
