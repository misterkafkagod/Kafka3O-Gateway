package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundCognitoCompletionFixture struct {
	*boundCognitoRefreshFixture
	result cognito.RefreshExchangeResult
	proof  cognito.VerifiedRefresh
}

func newBoundCognitoCompletionFixture(t *testing.T, mode string, authentication cognito.ClientAuthentication) *boundCognitoCompletionFixture {
	t.Helper()
	f := &boundCognitoCompletionFixture{boundCognitoRefreshFixture: newBoundCognitoRefreshFixtureWithAuthentication(t, authentication)}
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
	var err error
	f.result, err = f.exchanger.RefreshBoundCognitoSession(context.Background(), f.record.Session.TokenHash, f.binding, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	f.proof, err = f.result.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *boundCognitoCompletionFixture) completionRequest() cognito.RefreshCompletionRequest {
	return cognito.RefreshCompletionRequest{Refresh: f.proof, Protector: f.protector, OccurredAt: f.clock()}
}

func TestBoundCognitoRefreshExchangeCompletesVerifiedRotation(t *testing.T) {
	for _, authentication := range []cognito.ClientAuthentication{cognito.ClientAuthenticationPublic, cognito.ClientAuthenticationBasic} {
		for _, mode := range []string{"refresh no rotation", "refresh rotated"} {
			t.Run(string(authentication)+"/"+mode, func(t *testing.T) {
				f := newBoundCognitoCompletionFixture(t, mode, authentication)
				ctx := context.Background()
				request := f.completionRequest()
				wrong := sha256.Sum256([]byte("other browser marker"))
				if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("unbound completion accepted a bound proof: %v", err)
				}
				if err := f.db.CompleteBoundCognitoRefresh(ctx, request, [32]byte{}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("bound completion accepted a missing marker: %v", err)
				}
				if err := f.db.CompleteBoundCognitoRefresh(ctx, request, wrong); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("bound completion accepted a different marker: %v", err)
				}
				if _, err := f.db.UseBoundSession(ctx, f.record.Session.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
					t.Fatalf("uncompleted bound refresh granted session use: %v", err)
				}
				details, err := f.proof.Details(f.protector, f.clock())
				if err != nil || details.OperationID != f.result.OperationID || details.SessionID != f.credentials.Principal.SessionID {
					t.Fatalf("bound provider exchange returned an invalid proof: %v", err)
				}
				altered := f.result
				altered.OperationID = "untrusted-operation"
				alteredProof, err := altered.ForCompletion(f.protector, f.clock())
				if err != nil {
					t.Fatal(err)
				}
				alteredDetails, err := alteredProof.Details(f.protector, f.clock())
				if err != nil || alteredDetails.OperationID != f.result.OperationID {
					t.Fatal("caller changed the bound proof's operation")
				}
				if err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding); err != nil {
					t.Fatalf("complete bound provider refresh: %v", err)
				}
				var generation int64
				var groups string
				var encrypted []byte
				if err := f.db.db.QueryRow(`SELECT refresh_generation, groups_json, encrypted_refresh_token
FROM cognito_session_credentials WHERE session_id = ?`, details.SessionID).Scan(&generation, &groups, &encrypted); err != nil {
					t.Fatal(err)
				}
				wantToken := fixtureRefresh
				if mode == "refresh rotated" {
					wantToken = fixtureRotated
				}
				token, err := f.protector.Open(cognito.RefreshTokenBinding{
					SessionID: details.SessionID, IdentityID: details.IdentityID, Generation: 2,
				}, encrypted)
				if err != nil || token != wantToken || generation != 2 || groups != `["refreshed-group"]` ||
					bytes.Contains(encrypted, []byte(fixtureRefresh)) || bytes.Contains(encrypted, []byte(fixtureRotated)) {
					t.Fatalf("bound completion did not persist the verified protected generation: %v", err)
				}
				principal, err := f.db.UseBoundSession(ctx, f.record.Session.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector)
				if err != nil || principal.SessionID != details.SessionID || principal.Source != session.SourceCognito {
					t.Fatalf("completed bound refresh did not restore session use: %v", err)
				}
				if _, err := f.db.UseSession(ctx, f.record.Session.TokenHash, f.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
					t.Fatalf("unbound session use accepted a refreshed bound session: %v", err)
				}
				if err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
					t.Fatalf("bound completion proof was replayed: %v", err)
				}
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
				if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded {
					t.Fatalf("bound refresh was not atomically audited: count=%d err=%v", len(events), err)
				}
				serialized, err := json.Marshal([]any{f.result, f.proof, details, request})
				if err != nil {
					t.Fatal(err)
				}
				for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
					fixtureRefresh, fixtureRotated, fixtureAccess, "opaque|synthetic-subject", f.provider.Issuer, "unmapped-group", "refreshed-group"} {
					if strings.Contains(string(serialized), secret) || strings.Contains(fmt.Sprint(events), secret) {
						t.Fatal("bound refresh output or audit exposed sensitive material")
					}
				}
				f.reopen(t)
				if _, err := f.db.UseBoundSession(ctx, f.record.Session.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector); err != nil {
					t.Fatalf("refreshed bound session did not survive restart: %v", err)
				}
			})
		}
	}
}

func TestBoundCognitoRefreshExchangeRejectsWrongBrowserBeforeReservation(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	wrong := sha256.Sum256([]byte("other browser marker"))
	if result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, [32]byte{}, f.protector); !errors.Is(err, cognito.ErrRefreshState) || result.OperationID != "" {
		t.Fatalf("bound exchange accepted a missing marker: %v", err)
	}
	if result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, wrong, f.protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
		result.OperationID == "" || f.refreshCalls.Load() != 0 {
		t.Fatalf("bound exchange accepted a different marker: %v", err)
	}
	f.mu.Lock()
	f.mode = "refresh rotated"
	f.mu.Unlock()
	result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
	if err != nil || result.OperationID == "" || f.refreshCalls.Load() != 1 {
		t.Fatalf("wrong marker consumed the correct browser's reservation: %v", err)
	}
}

func TestBoundCognitoRefreshExchangePreservesUnboundIsolation(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	hash := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock()).TokenHash
	marker := sha256.Sum256([]byte("unrelated browser marker"))
	if _, err := f.exchanger.RefreshBoundCognitoSession(ctx, hash, marker, f.protector); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 0 {
		t.Fatalf("bound exchange accepted an unbound session: %v", err)
	}
	result, err := f.exchanger.RefreshCognitoSession(ctx, hash, f.protector)
	if err != nil {
		t.Fatalf("bound denial blocked the unbound exchange: %v", err)
	}
	proof, err := result.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	request := cognito.RefreshCompletionRequest{Refresh: proof, Protector: f.protector, OccurredAt: f.clock()}
	if err := f.db.CompleteBoundCognitoRefresh(ctx, request, marker); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("bound completion accepted an unbound proof: %v", err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, request); err != nil {
		t.Fatalf("bound proof rejection consumed an unbound proof: %v", err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); err != nil || f.refreshCalls.Load() != 1 {
		t.Fatalf("unbound refresh did not remain usable: %v", err)
	}
}

func TestBoundCognitoRefreshExchangeAndCompletionAreSingleUse(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	f.mu.Lock()
	f.mode = "refresh rotated"
	f.mu.Unlock()
	ctx := context.Background()
	const callers = 8
	type exchangeResult struct {
		result cognito.RefreshExchangeResult
		err    error
	}
	results := make(chan exchangeResult, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
			results <- exchangeResult{result: result, err: err}
		}()
	}
	wait.Wait()
	close(results)
	var successful cognito.RefreshExchangeResult
	success, denied := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			success++
			successful = result.result
		case errors.Is(result.err, cognito.ErrRefreshUnavailable):
			denied++
		default:
			t.Fatalf("unexpected bound provider exchange error: %v", result.err)
		}
	}
	if success != 1 || denied != callers-1 || f.refreshCalls.Load() != 1 {
		t.Fatalf("bound exchange was not reserved once: success=%d denied=%d", success, denied)
	}
	proof, err := successful.ForCompletion(f.protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	completion := cognito.RefreshCompletionRequest{Refresh: proof, Protector: f.protector, OccurredAt: f.clock()}
	completed := make(chan error, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			completed <- f.db.CompleteBoundCognitoRefresh(ctx, completion, f.binding)
		}()
	}
	wait.Wait()
	close(completed)
	success, denied = 0, 0
	for err := range completed {
		switch {
		case err == nil:
			success++
		case errors.Is(err, cognito.ErrRefreshUnavailable):
			denied++
		default:
			t.Fatalf("unexpected bound completion error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.refreshCalls.Load() != 1 {
		t.Fatalf("bound completion was not single use: success=%d denied=%d", success, denied)
	}
}
