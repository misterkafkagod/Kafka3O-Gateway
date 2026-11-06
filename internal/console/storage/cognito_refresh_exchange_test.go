package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func serveCognitoFixtureRefresh(t *testing.T, f *cognitoCodeFixture, w http.ResponseWriter, r *http.Request, mode, nonce string, authValid bool) {
	t.Helper()
	f.mu.RLock()
	expectedRefresh, entered, release := f.expectedRefresh, f.refreshEntered, f.refreshRelease
	f.mu.RUnlock()
	if r.Method != http.MethodPost || !authValid || r.Form.Get("refresh_token") != expectedRefresh ||
		r.Form.Get("code") != "" || r.Form.Get("code_verifier") != "" || r.Form.Get("redirect_uri") != "" {
		f.invalidCalls.Add(1)
		http.Error(w, "bad fixture refresh", http.StatusBadRequest)
		return
	}
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
	}
	switch mode {
	case "refresh outage":
		http.Error(w, fixtureRefresh, http.StatusServiceUnavailable)
		return
	case "refresh redirect":
		http.Redirect(w, r, f.server.URL+"/unexpected", http.StatusFound)
		return
	case "refresh disconnect":
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			f.invalidCalls.Add(1)
			http.Error(w, "fixture cannot disconnect", http.StatusInternalServerError)
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			f.invalidCalls.Add(1)
			return
		}
		connection.Close()
		return
	case "refresh oversized response":
		w.Write([]byte(strings.Repeat(" ", (1<<20)+1)))
		return
	case "refresh malformed JSON":
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{"))
		return
	}
	if advance := f.refreshAdvanceTo.Load(); advance != 0 {
		f.current.Store(advance)
	}
	now := f.clock()
	claims := map[string]any{
		"iss": f.provider.Issuer, "sub": "opaque|synthetic-subject", "aud": f.provider.ClientID,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		"token_use": "id", "cognito:groups": []string{"refreshed-group"},
	}
	key := f.privateKey
	switch mode {
	case "refresh matching nonce":
		claims["nonce"] = nonce
	case "refresh no groups":
		delete(claims, "cognito:groups")
	case "refresh short expiry":
		claims["exp"] = now.Add(2 * time.Minute).Unix()
	case "refresh wrong nonce":
		claims["nonce"] = "wrong-nonce"
	case "refresh wrong subject":
		claims["sub"] = "different-subject"
	case "refresh wrong audience":
		claims["aud"] = "different-client"
	case "refresh expired":
		claims["exp"] = now.Unix()
	case "refresh stale issuance":
		claims["iat"] = now.Add(-time.Minute).Unix()
	case "refresh malformed groups":
		claims["cognito:groups"] = "ADMIN"
	case "refresh forged":
		key = f.forgedKey
	}
	payload := map[string]any{
		"token_type": "Bearer", "access_token": fixtureAccess,
		"id_token": signCognitoCodeFixtureToken(t, key, claims), "expires_in": 3600,
	}
	switch mode {
	case "refresh rotated":
		payload["refresh_token"] = fixtureRotated
	case "refresh same token":
		payload["refresh_token"] = fixtureRefresh
	case "refresh missing ID":
		delete(payload, "id_token")
	case "refresh missing access":
		delete(payload, "access_token")
	case "refresh empty token":
		payload["refresh_token"] = ""
	case "refresh null token":
		payload["refresh_token"] = nil
	case "refresh invalid token":
		payload["refresh_token"] = fixtureRefresh + "\n"
	case "refresh oversized token":
		payload["refresh_token"] = strings.Repeat("x", cognito.MaxRefreshTokenBytes+1)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func TestCognitoRefreshExchangeProtectsRotationAndPrivateProof(t *testing.T) {
	for _, authentication := range []cognito.ClientAuthentication{cognito.ClientAuthenticationPublic, cognito.ClientAuthenticationBasic} {
		for _, mode := range []string{"refresh no rotation", "refresh rotated", "refresh same token", "refresh matching nonce"} {
			t.Run(string(authentication)+"/"+mode, func(t *testing.T) {
				f := newCognitoCodeFixture(t, "success", authentication)
				ctx := context.Background()
				start, exchange := cognitoSessionExchange(t, f)
				manager, protector := cognitoSessionTools(t, f)
				credentials, err := manager.IssueCognito(ctx, exchange)
				if err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				f.mode = mode
				f.mu.Unlock()
				hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
				result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
				if err != nil || result.OperationID == "" || f.tokenCalls.Load() != 2 || f.refreshCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
					t.Fatalf("one-shot provider refresh failed: %v", err)
				}
				originalID := result.OperationID
				result.OperationID = "untrusted-value"
				proof, err := result.ForCompletion(protector, f.clock())
				if err != nil {
					t.Fatal(err)
				}
				details, err := proof.Details(protector, f.clock())
				wantFreshUntil := time.Unix(f.clock().Unix(), 0).UTC().Add(5 * time.Minute)
				if err != nil || details.OperationID != originalID || details.SessionID != credentials.Principal.SessionID ||
					details.IdentityID != credentials.Principal.AccountID || details.PreviousGeneration != 1 ||
					details.Identity.Issuer != f.provider.Issuer || details.Identity.Subject != "opaque|synthetic-subject" ||
					!reflect.DeepEqual(details.Identity.Groups, []string{"refreshed-group"}) ||
					details.OriginalNonceHash != sha256.Sum256([]byte(start.Nonce)) ||
					!details.ClaimsFreshUntil.Equal(wantFreshUntil) || !details.ValidUntil.Equal(wantFreshUntil) {
					t.Fatalf("refresh proof did not preserve verified identity and deadlines: %v", err)
				}
				binding := cognito.RefreshTokenBinding{SessionID: details.SessionID, IdentityID: details.IdentityID, Generation: 2}
				wantToken := fixtureRefresh
				if mode == "refresh rotated" {
					wantToken = fixtureRotated
				}
				if token, err := protector.Open(binding, details.EncryptedRefreshToken); err != nil || token != wantToken {
					t.Fatalf("next-generation refresh token was not protected: %v", err)
				}
				if bytes.Contains(details.EncryptedRefreshToken, []byte(fixtureRefresh)) || bytes.Contains(details.EncryptedRefreshToken, []byte(fixtureRotated)) {
					t.Fatal("completion proof retained a plaintext provider token")
				}
				details.Identity.Groups[0] = "forged-group"
				details.Identity.Audience[0] = "forged-client"
				details.PreviousIdentity.Groups[0] = "forged-previous-group"
				details.PreviousIdentity.Audience[0] = "forged-previous-client"
				details.EncryptedRefreshToken[0] ^= 1
				again, err := proof.Details(protector, f.clock())
				if err != nil || !reflect.DeepEqual(again.Identity.Groups, []string{"refreshed-group"}) ||
					!reflect.DeepEqual(again.Identity.Audience, []string{f.provider.ClientID}) ||
					!reflect.DeepEqual(again.PreviousIdentity.Groups, []string{"unmapped-group"}) ||
					!reflect.DeepEqual(again.PreviousIdentity.Audience, []string{f.provider.ClientID}) {
					t.Fatalf("caller changed the verified refresh proof: %v", err)
				}
				if token, err := protector.Open(binding, again.EncryptedRefreshToken); err != nil || token != wantToken {
					t.Fatalf("caller changed protected refresh material: %v", err)
				}
				if _, err := result.ForCompletion(protector, again.ValidUntil); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("expired refresh proof was accepted: %v", err)
				}
				if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
					t.Fatalf("uncommitted refresh proof granted session access: %v", err)
				}
				if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
					t.Fatalf("provider refresh was replayed: %v", err)
				}
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
				if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditDenied {
					t.Fatalf("refresh reservation/replay audit was incorrect: count=%d err=%v", len(events), err)
				}
				body, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, start.Nonce, credentials.Token,
					"opaque|synthetic-subject", "refreshed-group"} {
					if bytes.Contains(body, []byte(secret)) || strings.Contains(fmt.Sprintf("%+v", events), secret) {
						t.Fatal("refresh result or audit exposed provider or session material")
					}
				}
			})
		}
	}
}

func TestCognitoRefreshExchangeRejectsProviderFailuresWithoutReplay(t *testing.T) {
	for _, tt := range []struct {
		mode string
		want error
	}{
		{"refresh outage", cognito.ErrRefreshUnknown},
		{"refresh redirect", cognito.ErrRefreshUnknown},
		{"refresh disconnect", cognito.ErrRefreshUnknown},
		{"refresh oversized response", cognito.ErrRefreshUnknown},
		{"refresh malformed JSON", cognito.ErrRefreshUnknown},
		{"refresh missing ID", cognito.ErrInvalidIDToken},
		{"refresh missing access", cognito.ErrRefreshUnknown},
		{"refresh empty token", cognito.ErrRefreshUnknown},
		{"refresh null token", cognito.ErrRefreshUnknown},
		{"refresh invalid token", cognito.ErrRefreshUnknown},
		{"refresh oversized token", cognito.ErrRefreshUnknown},
		{"refresh wrong nonce", cognito.ErrInvalidIDToken},
		{"refresh wrong subject", cognito.ErrInvalidIDToken},
		{"refresh wrong audience", cognito.ErrInvalidIDToken},
		{"refresh expired", cognito.ErrInvalidIDToken},
		{"refresh stale issuance", cognito.ErrInvalidIDToken},
		{"refresh malformed groups", cognito.ErrInvalidIDToken},
		{"refresh forged", cognito.ErrInvalidIDToken},
	} {
		t.Run(tt.mode, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, protector := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.mode = tt.mode
			f.mu.Unlock()
			hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
			result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
			if !errors.Is(err, tt.want) || result.OperationID == "" ||
				f.tokenCalls.Load() != 2 || f.refreshCalls.Load() != 1 || f.redirectCalls.Load() != 0 {
				t.Fatalf("invalid provider refresh was not rejected once: %v", err)
			}
			if _, proofErr := result.ForCompletion(protector, f.clock()); !errors.Is(proofErr, cognito.ErrRefreshUnavailable) {
				t.Fatalf("failed refresh returned a completion proof: %v", proofErr)
			}
			for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, credentials.Token} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("provider failure exposed credentials")
				}
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditUnknown ||
				events[0].OperationID != result.OperationID || events[1].OperationID != result.OperationID {
				t.Fatalf("uncertain refresh failure was not audited: count=%d err=%v", len(events), err)
			}
			if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("failed refresh left the session usable: %v", err)
			}
			if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
				f.refreshCalls.Load() != 1 {
				t.Fatalf("failed provider request was replayed: %v", err)
			}
		})
	}
}

func TestCognitoRefreshExchangeSerializesConcurrentRequests(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.mode = "refresh rotated"
	f.mu.Unlock()
	hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
	const callers = 12
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
			results <- err
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
			t.Fatalf("concurrent provider refresh failed unexpectedly: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.refreshCalls.Load() != 1 || f.tokenCalls.Load() != 2 {
		t.Fatalf("provider refresh was not serialized: success=%d denied=%d", success, denied)
	}
}

func TestCognitoRefreshExchangeRequiresTrustedConfigurationAndProof(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
	otherAttempts, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other")
	if err != nil {
		t.Fatal(err)
	}
	otherProvider, err := cognito.NewRefreshTokenProtector(otherAttempts, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, otherProvider); !errors.Is(err, cognito.ErrRefreshState) {
		t.Fatalf("mismatched provider configuration was accepted: %v", err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != nil || f.refreshCalls.Load() != 0 {
		t.Fatalf("invalid configuration consumed the session: %v", err)
	}
	if _, err := (cognito.RefreshExchangeResult{OperationID: "forged"}).ForCompletion(protector, f.clock()); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("forged refresh result produced a completion proof: %v", err)
	}
	result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.ForCompletion(otherProvider, f.clock()); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("refresh proof was accepted for another provider: %v", err)
	}
	wrongKey, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.ForCompletion(wrongKey, f.clock()); !errors.Is(err, cognito.ErrRefreshTokenProtection) {
		t.Fatalf("refresh proof was accepted with the wrong key: %v", err)
	}
}

func TestCognitoRefreshExchangeRechecksExpiryAroundProviderCall(t *testing.T) {
	for _, tt := range []struct {
		name        string
		stage       string
		wantCalls   int64
		wantOutcome AuditOutcome
	}{
		{"before send", "before", 0, AuditFailedBeforeSend},
		{"during provider", "provider", 1, AuditUnknown},
		{"during encryption", "encrypt", 1, AuditUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, protector := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			deadline := credentials.Principal.IdleExpiresAt
			switch tt.stage {
			case "before":
				calls := 0
				f.db.now = func() time.Time {
					current := f.clock()
					calls++
					if calls == 3 {
						f.current.Store(deadline.UnixNano())
					}
					return current
				}
			case "provider":
				f.refreshAdvanceTo.Store(deadline.UnixNano())
			case "encrypt":
				protector, err = cognito.NewRefreshTokenProtector(f.manager, advanceDuringEncryption{fixture: f, deadline: deadline}, bytes.Repeat([]byte{9}, 32))
				if err != nil {
					t.Fatal(err)
				}
			}
			hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
			result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
			if !errors.Is(err, cognito.ErrRefreshUnavailable) || result.OperationID == "" ||
				f.refreshCalls.Load() != tt.wantCalls {
				t.Fatalf("expired session returned a refresh proof or sent unexpectedly: %v", err)
			}
			if _, proofErr := result.ForCompletion(protector, f.clock()); !errors.Is(proofErr, cognito.ErrRefreshUnavailable) {
				t.Fatalf("expired refresh returned a completion proof: %v", proofErr)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != tt.wantOutcome ||
				events[0].OperationID != result.OperationID || events[1].OperationID != result.OperationID {
				t.Fatalf("expiry outcome was not audited: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoRefreshExchangeWithholdsProofOnFailureAudit(t *testing.T) {
	for _, commitFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%t", commitFailure), func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, protector := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.mode = "refresh outage"
			f.mu.Unlock()
			trigger := `CREATE TRIGGER reject_refresh_outcome BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'unknown'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			want := cognito.ErrRefreshAudit
			if commitFailure {
				trigger = `CREATE TABLE refresh_outcome_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER reject_refresh_outcome AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'unknown'
BEGIN INSERT INTO refresh_outcome_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
			result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
			if !errors.Is(err, want) || result.OperationID == "" || f.refreshCalls.Load() != 1 {
				t.Fatalf("failed refresh audit returned a proof: %v", err)
			}
			if _, proofErr := result.ForCompletion(protector, f.clock()); !errors.Is(proofErr, cognito.ErrRefreshUnavailable) {
				t.Fatalf("failed refresh audit returned a completion proof: %v", proofErr)
			}
			if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("audit failure left the session usable: %v", err)
			}
			if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
				f.refreshCalls.Load() != 1 {
				t.Fatalf("audit failure allowed the provider request to replay: %v", err)
			}
		})
	}
}

func TestCognitoRefreshExchangeCanceledAfterReservationDoesNotSend(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(context.Background(), exchange)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	f.db.now = func() time.Time {
		calls++
		if calls == 3 {
			cancel()
		}
		return f.clock()
	}
	hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
	result, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector)
	if !errors.Is(err, cognito.ErrRefreshAudit) || !errors.Is(err, context.Canceled) ||
		result.OperationID == "" || f.refreshCalls.Load() != 0 {
		t.Fatalf("canceled refresh was not reported without a provider request: %v", err)
	}
	if _, err := result.ForCompletion(protector, f.clock()); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("canceled refresh returned a completion proof: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("canceled refresh lost its durable reservation: count=%d err=%v", len(events), err)
	}
	if _, err := manager.Resolve(context.Background(), credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("canceled refresh left the session usable: %v", err)
	}
	if _, err := f.exchanger.RefreshCognitoSession(context.Background(), hash, protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
		f.refreshCalls.Load() != 0 {
		t.Fatalf("canceled refresh allowed automatic replay: %v", err)
	}
}

func TestCognitoRefreshFailureAuditRequiresMatchingReservation(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	request := cognitoRefreshRequest(t, credentials, protector, f.clock())
	reservation, err := f.db.ReserveCognitoRefresh(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	failure := cognito.RefreshFailure{
		OperationID: reservation.OperationID, SessionID: reservation.SessionID, Generation: reservation.Generation,
		OccurredAt: f.clock(), Status: cognito.RefreshOutcomeUnknown,
	}
	other := cognitoRefreshRequest(t, credentials, protector, f.clock()).OperationID
	for _, forged := range []cognito.RefreshFailure{
		{OperationID: other, SessionID: failure.SessionID, Generation: failure.Generation, OccurredAt: f.clock(), Status: failure.Status},
		{OperationID: failure.OperationID, SessionID: other, Generation: failure.Generation, OccurredAt: f.clock(), Status: failure.Status},
		{OperationID: failure.OperationID, SessionID: failure.SessionID, Generation: 2, OccurredAt: f.clock(), Status: failure.Status},
	} {
		if err := f.db.RecordCognitoRefreshFailure(ctx, forged); !errors.Is(err, cognito.ErrRefreshUnavailable) {
			t.Fatalf("failure audit accepted an unbound reservation: %v", err)
		}
	}
	if err := f.db.RecordCognitoRefreshFailure(ctx, failure); err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecordCognitoRefreshFailure(ctx, failure); !errors.Is(err, cognito.ErrRefreshAudit) {
		t.Fatalf("failure outcome was recorded twice: %v", err)
	}
}

func TestCognitoRefreshOutageDoesNotBlockLocalLogin(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	passwordHash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: passwordHash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.mode = "refresh outage"
	f.mu.Unlock()
	hash := cognitoRefreshRequest(t, credentials, protector, f.clock()).TokenHash
	if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, protector); !errors.Is(err, cognito.ErrRefreshUnknown) {
		t.Fatalf("provider outage was not reported: %v", err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, manager)
	if err != nil {
		t.Fatal(err)
	}
	local, err := login.Execute(ctx, "admin", "initial passphrase", netip.MustParseAddr("127.0.0.1"))
	if err != nil || local.Principal.Source != session.SourceLocal {
		t.Fatalf("provider outage blocked local login: %v", err)
	}
	if _, err := manager.Resolve(ctx, local.Token); err != nil {
		t.Fatalf("local login did not produce a usable session: %v", err)
	}
}
