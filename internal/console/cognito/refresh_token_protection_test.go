package cognito

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type protectionFixtureStore struct{}

func (protectionFixtureStore) CreateCognitoAttempt(context.Context, AttemptRecord) (time.Time, error) {
	return time.Time{}, ErrAttemptUnknown
}

func (protectionFixtureStore) ConsumeCognitoAttempt(context.Context, AttemptConsumption) (AttemptRecord, error) {
	return AttemptRecord{}, ErrAttemptUnknown
}

func (protectionFixtureStore) FailCognitoAttempt(context.Context, AttemptFailure) error {
	return ErrAttemptUnknown
}

func protectionFixtureManager(t *testing.T, provider ProviderConfig, callback string, key []byte) *AttemptManager {
	t.Helper()
	manager, err := NewAttemptManager(protectionFixtureStore{}, rand.Reader, time.Now, key, provider, callback)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func protectionFixtureProvider() ProviderConfig {
	return ProviderConfig{
		Issuer: "https://issuer.example/pool", ClientID: "synthetic-client",
		AuthorizationURL: "https://login.example/oauth2/authorize",
		TokenURL:         "https://login.example/oauth2/token",
		JWKSURL:          "https://issuer.example/pool/.well-known/jwks.json",
	}
}

func TestCognitoRefreshTokenProtection(t *testing.T) {
	provider := protectionFixtureProvider()
	attemptKey := bytes.Repeat([]byte{1}, 32)
	refreshKey := bytes.Repeat([]byte{2}, 32)
	manager := protectionFixtureManager(t, provider, "https://console.example/callback", attemptKey)
	protector, err := NewRefreshTokenProtector(manager, rand.Reader, refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := RefreshTokenBinding{SessionID: "synthetic-session", IdentityID: "synthetic-identity", Generation: 1}
	token := "synthetic-refresh-token"
	first, err := protector.Seal(binding, token)
	if err != nil {
		t.Fatal(err)
	}
	second, err := protector.Seal(binding, token)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(first, []byte(token)) || bytes.Equal(first, second) {
		t.Fatal("refresh-token ciphertext exposed plaintext or repeated its nonce")
	}
	if opened, err := protector.Open(binding, first); err != nil || opened != token {
		t.Fatalf("protected refresh token did not round-trip: %v", err)
	}
	restarted := protectionFixtureManager(t, provider, "https://console.example/callback", attemptKey)
	restartedProtector, err := NewRefreshTokenProtector(restarted, rand.Reader, refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := restartedProtector.Open(binding, first); err != nil || opened != token {
		t.Fatalf("protected refresh token could not be opened after restart: %v", err)
	}
	for _, tt := range []struct {
		name    string
		binding RefreshTokenBinding
	}{
		{"wrong session", RefreshTokenBinding{SessionID: "another-session", IdentityID: binding.IdentityID, Generation: 1}},
		{"wrong identity", RefreshTokenBinding{SessionID: binding.SessionID, IdentityID: "another-identity", Generation: 1}},
		{"wrong generation", RefreshTokenBinding{SessionID: binding.SessionID, IdentityID: binding.IdentityID, Generation: 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if opened, err := protector.Open(tt.binding, first); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
				t.Fatalf("refresh token was opened under another binding: %v", err)
			}
		})
	}
	tampered := bytes.Clone(first)
	tampered[len(tampered)-1] ^= 1
	if opened, err := protector.Open(binding, tampered); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("tampered refresh token was accepted: %v", err)
	}
	tampered = bytes.Clone(first)
	tampered[0] ^= 1
	if opened, err := protector.Open(binding, tampered); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("refresh token with a tampered nonce was accepted: %v", err)
	}
	shortBinding := RefreshTokenBinding{SessionID: "a", IdentityID: "bc", Generation: 1}
	shiftedBinding := RefreshTokenBinding{SessionID: "ab", IdentityID: "c", Generation: 1}
	shortCiphertext, err := protector.Seal(shortBinding, token)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := protector.Open(shiftedBinding, shortCiphertext); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("ambiguous refresh-token binding was accepted: %v", err)
	}
	otherKey, err := NewRefreshTokenProtector(manager, rand.Reader, bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := otherKey.Open(binding, first); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("different key opened the refresh token: %v", err)
	}
	otherProvider := provider
	otherProvider.ClientID = "another-client"
	otherManager := protectionFixtureManager(t, otherProvider, "https://console.example/callback", attemptKey)
	otherProtector, err := NewRefreshTokenProtector(otherManager, rand.Reader, refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := otherProtector.Open(binding, first); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("different provider opened the refresh token: %v", err)
	}
	otherManager = protectionFixtureManager(t, provider, "https://console.example/other-callback", attemptKey)
	otherProtector, err = NewRefreshTokenProtector(otherManager, rand.Reader, refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := otherProtector.Open(binding, first); !errors.Is(err, ErrRefreshTokenProtection) || opened != "" {
		t.Fatalf("different callback opened the refresh token: %v", err)
	}
}

func TestCognitoRefreshTokenProtectionRejectsInvalidInputs(t *testing.T) {
	attemptKey := bytes.Repeat([]byte{1}, 32)
	manager := protectionFixtureManager(t, protectionFixtureProvider(), "https://console.example/callback", attemptKey)
	refreshKey := bytes.Repeat([]byte{2}, 32)
	for _, tt := range []struct {
		name    string
		manager *AttemptManager
		random  io.Reader
		key     []byte
	}{
		{"missing manager", nil, rand.Reader, refreshKey},
		{"missing random", manager, nil, refreshKey},
		{"missing key", manager, rand.Reader, nil},
		{"short key", manager, rand.Reader, refreshKey[:31]},
		{"zero key", manager, rand.Reader, make([]byte, 32)},
		{"attempt key reuse", manager, rand.Reader, attemptKey},
		{"uninitialized manager", &AttemptManager{}, rand.Reader, refreshKey},
	} {
		t.Run(tt.name, func(t *testing.T) {
			protector, err := NewRefreshTokenProtector(tt.manager, tt.random, tt.key)
			if !errors.Is(err, ErrInvalidRefreshProtectionConfig) || protector != nil {
				t.Fatalf("invalid refresh-token key configuration was accepted: %v", err)
			}
		})
	}
	protector, err := NewRefreshTokenProtector(manager, rand.Reader, refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := RefreshTokenBinding{SessionID: "session", IdentityID: "identity", Generation: 1}
	for _, tt := range []struct {
		name    string
		binding RefreshTokenBinding
		token   string
	}{
		{"empty token", binding, ""},
		{"oversized token", binding, strings.Repeat("x", MaxRefreshTokenBytes+1)},
		{"control in token", binding, "abc\nsecret"},
		{"non-ASCII token", binding, "abc\u00e9"},
		{"empty session", RefreshTokenBinding{IdentityID: "identity", Generation: 1}, "token"},
		{"empty identity", RefreshTokenBinding{SessionID: "session", Generation: 1}, "token"},
		{"zero generation", RefreshTokenBinding{SessionID: "session", IdentityID: "identity"}, "token"},
		{"invalid ID", RefreshTokenBinding{SessionID: "session\n", IdentityID: "identity", Generation: 1}, "token"},
		{"oversized ID", RefreshTokenBinding{SessionID: strings.Repeat("s", 129), IdentityID: "identity", Generation: 1}, "token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ciphertext, err := protector.Seal(tt.binding, tt.token)
			if !errors.Is(err, ErrInvalidRefreshToken) || ciphertext != nil {
				t.Fatalf("invalid refresh-token input was accepted: %v", err)
			}
		})
	}
	if opened, err := protector.Open(binding, nil); !errors.Is(err, ErrInvalidRefreshToken) || opened != "" {
		t.Fatalf("empty ciphertext was accepted: %v", err)
	}
	if opened, err := protector.Open(binding, make([]byte, MaxRefreshTokenBytes+29)); !errors.Is(err, ErrInvalidRefreshToken) || opened != "" {
		t.Fatalf("oversized ciphertext was accepted: %v", err)
	}
	maximum := strings.Repeat("x", MaxRefreshTokenBytes)
	ciphertext, err := protector.Seal(binding, maximum)
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := protector.Open(binding, ciphertext); err != nil || opened != maximum {
		t.Fatalf("maximum-sized refresh token failed: %v", err)
	}
	failedRandom, err := NewRefreshTokenProtector(manager, bytes.NewReader(nil), refreshKey)
	if err != nil {
		t.Fatal(err)
	}
	if ciphertext, err := failedRandom.Seal(binding, "token"); !errors.Is(err, io.EOF) || !errors.Is(err, ErrRefreshTokenProtection) || ciphertext != nil {
		t.Fatalf("failed randomness did not prevent encryption: %v", err)
	}
}

func TestCognitoRefreshTokenProtectionConcurrentUse(t *testing.T) {
	manager := protectionFixtureManager(t, protectionFixtureProvider(), "https://console.example/callback", bytes.Repeat([]byte{1}, 32))
	protector, err := NewRefreshTokenProtector(manager, rand.Reader, bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	binding := RefreshTokenBinding{SessionID: "session", IdentityID: "identity", Generation: 1}
	const workers = 32
	results := make(chan []byte, workers)
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ciphertext, err := protector.Seal(binding, "synthetic-token")
			if err != nil {
				failures <- err
				return
			}
			opened, err := protector.Open(binding, ciphertext)
			if err != nil || opened != "synthetic-token" {
				failures <- ErrRefreshTokenProtection
				return
			}
			results <- ciphertext
		}()
	}
	wait.Wait()
	close(failures)
	close(results)
	for err := range failures {
		t.Errorf("concurrent token protection failed: %v", err)
	}
	seen := make(map[string]bool)
	for ciphertext := range results {
		nonce := string(ciphertext[:protector.aead.NonceSize()])
		if seen[nonce] {
			t.Error("concurrent encryption reused a nonce")
		}
		seen[nonce] = true
	}
	if len(seen) != workers {
		t.Fatalf("protected %d of %d tokens", len(seen), workers)
	}
}
