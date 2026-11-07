package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBrowserMarkerBindsLocalSessionAndCognitoExchange(t *testing.T) {
	f, _, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	value, generatedHash, err := session.NewBrowserMarker(bytes.NewReader(bytes.Repeat([]byte{31}, 32)))
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "https://console.example/callback", nil)
	request.Header.Set("Cookie", session.BrowserMarkerCookieName+"="+value)
	hash, err := session.ReadBrowserMarkerCookie(request)
	if err != nil || hash != generatedHash {
		t.Fatalf("generated marker did not round-trip through its cookie: %v", err)
	}
	local, _ := browserSessionRecord("marker-local", "browser-local-a", hash, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, local); err != nil {
		t.Fatal(err)
	}
	cognitoRecord, _, _ := browserCognitoRecord(t, f, "marker-cognito", hash)
	if principal, err := f.db.CreateCognitoSession(ctx, cognitoRecord); err != session.ErrBrowserSessionConflict || principal.SessionID != "" {
		t.Fatalf("bound Cognito exchange replaced the active local browser session: %v", err)
	}
	if principal, err := f.db.UseBoundSession(ctx, local.TokenHash, hash, f.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != local.SessionID {
		t.Fatalf("bound Cognito denial changed the local session: %v", err)
	}
	other := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{32}, 32))
	request.Header.Set("Cookie", session.BrowserMarkerCookieName+"="+other)
	otherHash, err := session.ReadBrowserMarkerCookie(request)
	if err != nil || otherHash == hash {
		t.Fatal("another marker did not receive an independent digest")
	}
	otherLocal, _ := browserSessionRecord("marker-other", "browser-local-b", otherHash, f.clock())
	if principal, err := f.db.CreateLocalSession(ctx, otherLocal); err != nil || principal.SessionID != otherLocal.SessionID {
		t.Fatalf("one browser incorrectly blocked another browser: %v", err)
	}
}

func TestBrowserMarkerCannotReuseSessionOrCSRFValues(t *testing.T) {
	for _, mode := range []string{"session", "csrf"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _ := newBrowserSessionFixture(t)
			record, token := browserSessionRecord("marker-value-reuse", "browser-local-a", [32]byte{}, f.clock())
			value := token
			if mode == "csrf" {
				raw := bytes.Repeat([]byte{37}, 32)
				record.CSRFHash = sha256.Sum256(raw)
				value = base64.RawURLEncoding.EncodeToString(raw)
			}
			hash, err := session.HashBrowserMarker(value)
			if err != nil {
				t.Fatal(err)
			}
			record.BrowserBindingHash = hash
			if principal, err := f.db.CreateLocalSession(context.Background(), record); !errors.Is(err, session.ErrInvalidSessionConfig) || principal.SessionID != "" {
				t.Fatalf("reused %s value produced a browser session: %v", mode, err)
			}
		})
	}
}
