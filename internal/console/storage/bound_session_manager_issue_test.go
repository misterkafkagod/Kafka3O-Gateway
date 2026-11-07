package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerIssueLocalConflictAndReplacement(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	manager, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	first := identity.LocalAccount{ID: "browser-local-a", Enabled: true, CredentialVersion: 1}
	second := identity.LocalAccount{ID: "browser-local-b", Enabled: true, CredentialVersion: 1}
	if credentials, err := manager.IssueBoundLocal(ctx, first, [32]byte{}); err != session.ErrInvalidSessionConfig ||
		credentials.Token != "" || credentials.CSRFToken != "" {
		t.Fatalf("bound local issuance accepted a missing marker: %v", err)
	}
	credentials, err := manager.IssueBoundLocal(ctx, first, binding)
	if err != nil || credentials.Principal.AccountID != first.ID || credentials.Principal.Source != session.SourceLocal ||
		credentials.Token == "" || credentials.CSRFToken == "" || credentials.Token == credentials.CSRFToken {
		t.Fatalf("bound local issuance failed: %v", err)
	}
	if principal, err := manager.ResolveBound(ctx, credentials.Token, binding); err != nil || principal.SessionID != credentials.Principal.SessionID {
		t.Fatalf("issued bound local session did not resolve: %v", err)
	}
	wrong := sha256.Sum256([]byte("other issued local browser"))
	if _, err := manager.ResolveBound(ctx, credentials.Token, wrong); err != session.ErrInvalidSession {
		t.Fatalf("issued local session resolved for another marker: %v", err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != session.ErrInvalidSession {
		t.Fatalf("unbound manager accepted an issued bound local session: %v", err)
	}
	if denied, err := manager.IssueBoundLocal(ctx, second, binding); err != session.ErrBrowserSessionConflict ||
		denied.Token != "" || denied.CSRFToken != "" || denied.Principal.SessionID != "" {
		t.Fatalf("active bound local session was silently replaced: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.start", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditSucceeded || events[1].Outcome != AuditDenied {
		t.Fatalf("bound local conflict was not durably audited: count=%d err=%v", len(events), err)
	}
	serialized, err := json.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{marker, base64.RawURLEncoding.EncodeToString(binding[:]), credentials.Token, credentials.CSRFToken} {
		if strings.Contains(string(serialized), secret) || strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("bound local issuance exposed a browser or session credential")
		}
	}
	f.reopen(t)
	manager, err = session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if principal, err := manager.ResolveBound(ctx, credentials.Token, binding); err != nil || principal.SessionID != credentials.Principal.SessionID {
		t.Fatalf("issued bound local session did not survive restart: %v", err)
	}
	if err := manager.RevokeBound(ctx, credentials.Token, binding); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.IssueBoundLocal(ctx, second, binding)
	if err != nil || replacement.Principal.AccountID != second.ID {
		t.Fatalf("revoked browser could not receive a replacement: %v", err)
	}
	if _, err := manager.ResolveBound(ctx, credentials.Token, binding); err != session.ErrInvalidSession {
		t.Fatalf("replaced bound local session revived: %v", err)
	}
	if principal, err := manager.ResolveBound(ctx, replacement.Token, binding); err != nil || principal.SessionID != replacement.Principal.SessionID {
		t.Fatalf("replacement bound local session was unavailable: %v", err)
	}
}

func TestBoundSessionManagerIssueCognitoOneShotAndRestart(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	if credentials, err := manager.IssueBoundCognito(ctx, exchange, [32]byte{}); err != session.ErrInvalidSessionConfig ||
		credentials.Token != "" || credentials.CSRFToken != "" {
		t.Fatalf("bound Cognito issuance accepted a missing marker: %v", err)
	}
	credentials, err := manager.IssueBoundCognito(ctx, exchange, binding)
	if err != nil || credentials.Principal.Source != session.SourceCognito || credentials.Token == "" || credentials.CSRFToken == "" {
		t.Fatalf("bound Cognito issuance failed after missing-marker rejection: %v", err)
	}
	wrong := sha256.Sum256([]byte("other issued Cognito browser"))
	if _, err := manager.ResolveBound(ctx, credentials.Token, wrong); err != session.ErrInvalidSession {
		t.Fatalf("issued Cognito session resolved for another marker: %v", err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != session.ErrInvalidSession {
		t.Fatalf("unbound manager accepted an issued bound Cognito session: %v", err)
	}
	if denied, err := manager.IssueBoundCognito(ctx, exchange, wrong); !errors.Is(err, session.ErrCognitoSessionUnavailable) ||
		denied.Token != "" || denied.CSRFToken != "" {
		t.Fatalf("bound Cognito exchange proof was replayed: %v", err)
	}
	var generation int64
	var encrypted, savedMarker []byte
	if err := f.db.db.QueryRow(`SELECT credentials.refresh_generation, credentials.encrypted_refresh_token, sessions.browser_binding_hash
FROM cognito_session_credentials AS credentials JOIN console_sessions AS sessions ON sessions.id = credentials.session_id
WHERE sessions.id = ?`, credentials.Principal.SessionID).Scan(&generation, &encrypted, &savedMarker); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || !bytes.Equal(savedMarker, binding[:]) || bytes.Contains(encrypted, []byte(fixtureRefresh)) {
		t.Fatal("issued Cognito session did not protect its marker and provider credential")
	}
	if token, err := protector.Open(cognito.RefreshTokenBinding{
		SessionID: credentials.Principal.SessionID, IdentityID: credentials.Principal.AccountID, Generation: generation,
	}, encrypted); err != nil || token != fixtureRefresh {
		t.Fatalf("issued Cognito provider credential was not correctly protected: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.session", Limit: 10})
	if err != nil || len(events) != 3 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded || events[2].Outcome != AuditDenied {
		t.Fatalf("bound Cognito issuance and replay were not audited: count=%d err=%v", len(events), err)
	}
	output, err := json.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{marker, base64.RawURLEncoding.EncodeToString(binding[:]), credentials.Token, credentials.CSRFToken,
		f.provider.Issuer, "opaque|synthetic-subject", fixtureRefresh, "unmapped-group"} {
		if strings.Contains(string(output), secret) || strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("bound Cognito issuance exposed a credential or verified claim")
		}
	}
	f.reopen(t)
	f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	manager, _ = cognitoSessionTools(t, f)
	if principal, err := manager.ResolveBound(ctx, credentials.Token, binding); err != nil || principal.SessionID != credentials.Principal.SessionID {
		t.Fatalf("issued bound Cognito session did not survive restart: %v", err)
	}
}

func TestBoundSessionManagerIssueRejectsInvalidCognitoProof(t *testing.T) {
	for _, mode := range []string{"empty-proof", "provider-outage"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			manager, _ := cognitoSessionTools(t, f)
			var exchange cognito.CodeExchangeResult
			var calls int64
			if mode == "provider-outage" {
				f.mu.Lock()
				f.mode = "outage"
				f.mu.Unlock()
				_, consumed := f.consumeAttempt(t)
				var err error
				exchange, err = f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode)
				if err == nil {
					t.Fatal("outage fixture unexpectedly completed provider exchange")
				}
				calls = 1
			}
			if credentials, err := manager.IssueBoundCognito(ctx, exchange, binding); err == nil ||
				credentials.Token != "" || credentials.CSRFToken != "" || credentials.Principal.SessionID != "" || f.tokenCalls.Load() != calls {
				t.Fatalf("unverified Cognito proof created a bound session: %v", err)
			}
			var count int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions").Scan(&count); err != nil || count != 0 {
				t.Fatalf("invalid proof persisted a session: %v", err)
			}
		})
	}
}

func TestBoundSessionManagerIssueCognitoConflictConsumesProof(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	local, err := manager.IssueBoundLocal(ctx, identity.LocalAccount{ID: "browser-local-a", Enabled: true, CredentialVersion: 1}, binding)
	if err != nil {
		t.Fatal(err)
	}
	denied, err := manager.IssueBoundCognito(ctx, exchange, binding)
	if err != session.ErrBrowserSessionConflict || denied.Token != "" || denied.CSRFToken != "" || denied.Principal.SessionID != "" {
		t.Fatalf("bound Cognito issuance replaced an active local session: %v", err)
	}
	if principal, err := manager.ResolveBound(ctx, local.Token, binding); err != nil || principal.SessionID != local.Principal.SessionID {
		t.Fatalf("denied Cognito issuance changed the local session: %v", err)
	}
	if err := manager.RevokeBound(ctx, local.Token, binding); err != nil {
		t.Fatal(err)
	}
	if replay, err := manager.IssueBoundCognito(ctx, exchange, binding); !errors.Is(err, session.ErrCognitoSessionUnavailable) ||
		replay.Token != "" || replay.CSRFToken != "" || f.tokenCalls.Load() != 1 {
		t.Fatalf("denied Cognito proof was replayed after local logout: %v", err)
	}
}
