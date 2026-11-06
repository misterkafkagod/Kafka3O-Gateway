package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type cognitoCompletionFixture struct {
	*cognitoCodeFixture
	manager     *session.Manager
	protector   *cognito.RefreshTokenProtector
	credentials session.Credentials
	result      cognito.RefreshExchangeResult
	proof       cognito.VerifiedRefresh
	details     cognito.RefreshCompletionDetails
}

func newCognitoCompletionFixture(t *testing.T, mode string, auth cognito.ClientAuthentication) *cognitoCompletionFixture {
	t.Helper()
	f := &cognitoCompletionFixture{cognitoCodeFixture: newCognitoCodeFixture(t, "success", auth)}
	_, exchange := cognitoSessionExchange(t, f.cognitoCodeFixture)
	f.manager, f.protector = cognitoSessionTools(t, f.cognitoCodeFixture)
	var err error
	f.credentials, err = f.manager.IssueCognito(context.Background(), exchange)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
	hash := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock()).TokenHash
	f.result, err = f.exchanger.RefreshCognitoSession(context.Background(), hash, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	f.proof, err = f.result.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	f.details, err = f.proof.Details(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cognitoCompletionFixture) request() cognito.RefreshCompletionRequest {
	return cognito.RefreshCompletionRequest{Refresh: f.proof, Protector: f.protector, OccurredAt: f.clock()}
}

func TestCognitoRefreshCompletionPersistsVerifiedRotation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		mode        string
		auth        cognito.ClientAuthentication
		token       string
		groups      []string
		freshPeriod time.Duration
	}{
		{"public retained", "refresh no rotation", cognito.ClientAuthenticationPublic, fixtureRefresh, []string{"refreshed-group"}, 5 * time.Minute},
		{"confidential rotated", "refresh rotated", cognito.ClientAuthenticationBasic, fixtureRotated, []string{"refreshed-group"}, 5 * time.Minute},
		{"missing groups", "refresh no groups", cognito.ClientAuthenticationPublic, fixtureRefresh, []string{}, 5 * time.Minute},
		{"token expires first", "refresh short expiry", cognito.ClientAuthenticationPublic, fixtureRefresh, []string{"refreshed-group"}, 2 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCompletionFixture(t, tt.mode, tt.auth)
			ctx := context.Background()
			if _, err := f.manager.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("uncompleted refresh granted access: %v", err)
			}
			wantFreshUntil := time.Unix(f.clock().Unix(), 0).UTC().Add(tt.freshPeriod)
			if !f.details.ClaimsFreshUntil.Equal(wantFreshUntil) || !f.details.ValidUntil.Equal(wantFreshUntil) {
				t.Fatal("refresh proof did not use the exact earliest freshness deadline")
			}
			if err := f.db.CompleteCognitoRefresh(ctx, f.request()); err != nil {
				t.Fatalf("complete verified refresh: %v", err)
			}
			var generation, issued, expiry, freshUntil, idle int64
			var nonce, encrypted []byte
			var audienceJSON, groupsJSON string
			if err := f.db.db.QueryRowContext(ctx, `SELECT refresh_generation, original_nonce_hash,
audience_json, groups_json, issued_at_ns, token_expires_at_ns, claims_fresh_until_ns,
encrypted_refresh_token, console_sessions.idle_expires_at_ns
FROM cognito_session_credentials JOIN console_sessions ON console_sessions.id = cognito_session_credentials.session_id
WHERE session_id = ?`, f.credentials.Principal.SessionID).Scan(
				&generation, &nonce, &audienceJSON, &groupsJSON, &issued, &expiry, &freshUntil, &encrypted, &idle,
			); err != nil {
				t.Fatal(err)
			}
			var audience, groups []string
			if json.Unmarshal([]byte(audienceJSON), &audience) != nil ||
				json.Unmarshal([]byte(groupsJSON), &groups) != nil || groups == nil ||
				!reflect.DeepEqual(audience, []string{f.provider.ClientID}) || !reflect.DeepEqual(groups, tt.groups) ||
				generation != 2 || !bytes.Equal(nonce, f.details.OriginalNonceHash[:]) ||
				issued != f.details.Identity.IssuedAt.UnixNano() || expiry != f.details.Identity.ExpiresAt.UnixNano() ||
				freshUntil != wantFreshUntil.UnixNano() || idle != f.credentials.Principal.IdleExpiresAt.UnixNano() {
				t.Fatal("refresh completion did not atomically persist the verified snapshot and deadline")
			}
			binding := cognito.RefreshTokenBinding{
				SessionID: f.details.SessionID, IdentityID: f.details.IdentityID, Generation: generation,
			}
			if token, err := f.protector.Open(binding, encrypted); err != nil || token != tt.token {
				t.Fatalf("persisted next-generation credential was not correctly protected: %v", err)
			}
			if bytes.Contains(encrypted, []byte(fixtureRefresh)) || bytes.Contains(encrypted, []byte(fixtureRotated)) {
				t.Fatal("refresh completion stored a plaintext provider token")
			}
			var grants int
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE account_id = ?", f.details.IdentityID).Scan(&grants); err != nil || grants != 0 {
				t.Fatalf("refresh created a direct role grant: count=%d err=%v", grants, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded ||
				events[0].OperationID != f.result.OperationID || events[1].OperationID != f.result.OperationID ||
				events[1].ActorID != f.details.IdentityID || events[1].Source != AuditSourceCognito ||
				events[1].ResourceID != f.details.SessionID {
				t.Fatalf("refresh completion audit was incorrect: count=%d err=%v", len(events), err)
			}
			if err := f.db.CompleteCognitoRefresh(ctx, f.request()); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
				t.Fatalf("completion proof was replayed: %v", err)
			}
			principal, err := f.manager.Resolve(ctx, f.credentials.Token)
			if err != nil || principal.Source != session.SourceCognito || principal.AccountID != f.details.IdentityID {
				t.Fatalf("committed refresh did not restore session use: %v", err)
			}
			for _, path := range []string{f.path, f.path + "-wal"} {
				data, readErr := os.ReadFile(path)
				if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
					t.Fatal(readErr)
				}
				for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, f.credentials.Token, f.credentials.CSRFToken} {
					if bytes.Contains(data, []byte(secret)) {
						t.Fatal("refresh completion stored a provider or session credential as plaintext")
					}
				}
			}
			f.reopen(t)
			f.manager, f.protector = cognitoSessionTools(t, f.cognitoCodeFixture)
			if token, err := f.protector.Open(binding, encrypted); err != nil || token != tt.token {
				t.Fatalf("rotated credential did not survive restart: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, f.credentials.Token); err != nil {
				t.Fatalf("refreshed session did not survive restart: %v", err)
			}
			if err := f.manager.Revoke(ctx, f.credentials.Token); err != nil {
				t.Fatalf("refreshed session could not be revoked: %v", err)
			}
			var retained int
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
				f.details.SessionID).Scan(&retained); err != nil || retained != 0 {
				t.Fatalf("revocation retained refreshed provider credentials: count=%d err=%v", retained, err)
			}
		})
	}
}

func TestCognitoRefreshCompletionSupportsRepeatedGenerations(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	if err := f.db.CompleteCognitoRefresh(ctx, f.request()); err != nil {
		t.Fatal(err)
	}
	f.current.Store(f.clock().Add(time.Second).UnixNano())
	f.mu.Lock()
	f.mode, f.expectedRefresh = "refresh no rotation", fixtureRotated
	f.mu.Unlock()
	hash := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock()).TokenHash
	second, err := f.exchanger.RefreshCognitoSession(ctx, hash, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := second.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	details, err := proof.Details(f.protector, f.clock())
	if err != nil || details.PreviousGeneration != 2 {
		t.Fatalf("second refresh did not use the committed generation: %v", err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, cognito.RefreshCompletionRequest{
		Refresh: proof, Protector: f.protector, OccurredAt: f.clock(),
	}); err != nil {
		t.Fatalf("second generation could not complete: %v", err)
	}
	var generation int64
	var encrypted []byte
	if err := f.db.db.QueryRowContext(ctx, `SELECT refresh_generation, encrypted_refresh_token
FROM cognito_session_credentials WHERE session_id = ?`, f.credentials.Principal.SessionID).Scan(&generation, &encrypted); err != nil {
		t.Fatal(err)
	}
	if generation != 3 || f.refreshCalls.Load() != 2 || f.invalidCalls.Load() != 0 {
		t.Fatalf("successive generations were not committed once: generation=%d calls=%d", generation, f.refreshCalls.Load())
	}
	binding := cognito.RefreshTokenBinding{SessionID: details.SessionID, IdentityID: details.IdentityID, Generation: 3}
	if token, err := f.protector.Open(binding, encrypted); err != nil || token != fixtureRotated {
		t.Fatalf("rotated token was not retained across the next refresh: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 4 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded ||
		events[2].Outcome != AuditAttempted || events[3].Outcome != AuditSucceeded ||
		events[0].OperationID == events[2].OperationID {
		t.Fatalf("successive refresh generations were not independently audited: count=%d err=%v", len(events), err)
	}
	if _, err := f.manager.Resolve(ctx, f.credentials.Token); err != nil {
		t.Fatalf("successive refreshes did not restore session access: %v", err)
	}
}

func TestCognitoRefreshCompletionConsumesProofUnderConcurrency(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	const callers = 12
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- f.db.CompleteCognitoRefresh(context.Background(), f.request())
		}()
	}
	wait.Wait()
	close(results)
	success, denied := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, cognito.ErrRefreshUnavailable):
			denied++
		default:
			t.Fatalf("unexpected concurrent completion error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.refreshCalls.Load() != 1 {
		t.Fatalf("completion was not one-shot: success=%d denied=%d", success, denied)
	}
}

func TestCognitoRefreshCompletionPreservesLocalSessions(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh no rotation", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	passwordHash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: passwordHash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	local, err := f.manager.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, local.Token); err != nil {
		t.Fatalf("pending refresh blocked local session: %v", err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, f.request()); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.manager.Resolve(ctx, local.Token); err != nil || principal.Source != session.SourceLocal {
		t.Fatalf("completed refresh changed a local session: %v", err)
	}
}
