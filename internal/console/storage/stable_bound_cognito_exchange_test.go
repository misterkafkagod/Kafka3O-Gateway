package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestStableBoundCognitoExchangeMarkerIsolationAndIssuance(t *testing.T) {
	for _, authentication := range []cognito.ClientAuthentication{cognito.ClientAuthenticationPublic, cognito.ClientAuthenticationBasic} {
		t.Run(string(authentication), func(t *testing.T) {
			f, browserHash, marker := newBrowserSessionFixtureWithAuthentication(t, authentication)
			ctx := context.Background()
			_, wrongHash := stableCognitoAttemptMarker(11)
			start, consumed := boundCognitoSessionAttempt(t, f, browserHash)
			for _, attempt := range []struct {
				name string
				call func() (cognito.CodeExchangeResult, error)
			}{
				{"wrong marker", func() (cognito.CodeExchangeResult, error) {
					return f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, wrongHash)
				}},
				{"missing marker", func() (cognito.CodeExchangeResult, error) {
					return f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, [32]byte{})
				}},
				{"unbound exchanger", func() (cognito.CodeExchangeResult, error) {
					return f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode)
				}},
				{"nil context", func() (cognito.CodeExchangeResult, error) {
					return f.exchanger.ExchangeBoundAuthorizationCode(nil, consumed, fixtureCode, browserHash)
				}},
				{"forged attempt", func() (cognito.CodeExchangeResult, error) {
					forged := cognito.ConsumedAuthorization{OperationID: consumed.OperationID, NonceHash: consumed.NonceHash, CodeVerifier: consumed.CodeVerifier}
					return f.exchanger.ExchangeBoundAuthorizationCode(ctx, forged, fixtureCode, browserHash)
				}},
			} {
				result, err := attempt.call()
				if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || result.RefreshToken != "" {
					t.Fatalf("%s was accepted: %v", attempt.name, err)
				}
			}
			for _, request := range []cognito.CodeExchangeReservation{
				{OperationID: start.OperationID, DenialID: "wrong-marker", Attempt: consumed, BrowserBindingHash: wrongHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt},
				{OperationID: start.OperationID, DenialID: "missing-marker", Attempt: consumed, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt},
				{OperationID: start.OperationID, DenialID: "forged-attempt", BrowserBindingHash: browserHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt},
				{OperationID: "another-operation", DenialID: "wrong-operation", Attempt: consumed, BrowserBindingHash: browserHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt},
				{OperationID: start.OperationID, DenialID: "wrong-expiry", Attempt: consumed, BrowserBindingHash: browserHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt.Add(time.Second)},
			} {
				if _, err := f.db.ReserveCognitoCodeExchange(ctx, request); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
					t.Fatalf("direct reservation bypassed the bound proof: %v", err)
				}
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
			if err != nil || len(events) != 0 || f.tokenCalls.Load() != 0 {
				t.Fatalf("rejected proofs reserved or contacted the provider: events=%d err=%v", len(events), err)
			}

			exchange, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash)
			if err != nil || exchange.OperationID != start.OperationID || exchange.RefreshToken != fixtureRefresh ||
				f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
				t.Fatalf("valid bound code exchange failed: %v", err)
			}
			if _, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 1 {
				t.Fatalf("bound code exchange was replayed: %v", err)
			}
			manager, protector := cognitoSessionTools(t, f)
			if _, err := exchange.ForSession(protector, f.clock()); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("unbound proof access accepted a bound exchange: %v", err)
			}
			for _, hash := range [][32]byte{{}, wrongHash} {
				if _, err := exchange.ForBoundSession(protector, f.clock(), hash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
					t.Fatalf("bound proof access accepted a wrong marker: %v", err)
				}
				if credentials, err := manager.IssueBoundCognito(ctx, exchange, hash); err == nil || credentials.Token != "" || credentials.CSRFToken != "" {
					t.Fatalf("bound session accepted a wrong marker: %v", err)
				}
			}
			if credentials, err := manager.IssueCognito(ctx, exchange); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || credentials.Token != "" {
				t.Fatalf("unbound session accepted a bound proof: %v", err)
			}
			proof, err := exchange.ForBoundSession(protector, f.clock(), browserHash)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := proof.Details(protector, f.clock()); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("unbound details accepted a bound proof: %v", err)
			}
			for _, hash := range [][32]byte{{}, wrongHash} {
				if _, err := proof.BoundDetails(protector, f.clock(), hash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
					t.Fatalf("bound details accepted a wrong marker: %v", err)
				}
			}
			exchange.OperationID, exchange.RefreshToken, exchange.Identity.Subject = "forged", "forged", "forged"
			exchange.Identity.Audience[0] = "forged"
			details, err := proof.BoundDetails(protector, f.clock(), browserHash)
			if err != nil || details.OperationID != start.OperationID || details.RefreshToken != fixtureRefresh ||
				details.Identity.Subject != "opaque|synthetic-subject" || details.Identity.Audience[0] != f.provider.ClientID {
				t.Fatalf("mutable result changed the private bound proof: %v", err)
			}
			details.Identity.Audience[0] = "changed-copy"
			if again, err := proof.BoundDetails(protector, f.clock(), browserHash); err != nil || again.Identity.Audience[0] != f.provider.ClientID {
				t.Fatal("bound proof returned mutable identity aliases")
			}
			if _, err := proof.BoundDetails(protector, details.ValidUntil, browserHash); !errors.Is(err, cognito.ErrCodeExchangeExpired) {
				t.Fatalf("expired bound proof was accepted: %v", err)
			}
			if _, err := proof.BoundDetails(protector, f.clock().Add(-time.Second), browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("bound proof accepted a backwards clock: %v", err)
			}
			otherManager, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other")
			if err != nil {
				t.Fatal(err)
			}
			otherProtector, err := cognito.NewRefreshTokenProtector(otherManager, rand.Reader, bytes.Repeat([]byte{9}, 32))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := proof.BoundDetails(otherProtector, f.clock(), browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("bound proof accepted another provider binding: %v", err)
			}
			credentials, err := manager.IssueBoundCognito(ctx, exchange, browserHash)
			if err != nil || credentials.Principal.Source != session.SourceCognito || credentials.Token == "" || credentials.CSRFToken == "" {
				t.Fatalf("bound proof did not issue a session: %v", err)
			}
			if replay, err := manager.IssueBoundCognito(ctx, exchange, browserHash); !errors.Is(err, session.ErrCognitoSessionUnavailable) || replay.Token != "" {
				t.Fatalf("bound session proof was replayed: %v", err)
			}
			events, err = f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 30})
			if err != nil {
				t.Fatal(err)
			}
			output, err := json.Marshal([]any{exchange, proof, credentials, events})
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{marker, hex.EncodeToString(browserHash[:]), base64.RawURLEncoding.EncodeToString(browserHash[:]),
				start.State, start.Nonce, start.BrowserBinding, consumed.CodeVerifier, fixtureCode, fixtureRefresh,
				credentials.Token, credentials.CSRFToken, f.provider.Issuer, "opaque|synthetic-subject", "unmapped-group"} {
				if bytes.Contains(output, []byte(secret)) || strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("bound exchange or issuance exposed a credential, marker or verified claim")
				}
			}
			for _, path := range []string{f.path, f.path + "-wal"} {
				data, err := os.ReadFile(path)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				for _, secret := range []string{marker, start.State, start.Nonce, start.BrowserBinding, consumed.CodeVerifier,
					fixtureCode, fixtureRefresh, credentials.Token, credentials.CSRFToken} {
					if bytes.Contains(data, []byte(secret)) {
						t.Fatal("bound exchange stored a credential in plaintext")
					}
				}
			}
			f.reopen(t)
			secret := ""
			if authentication == cognito.ClientAuthenticationBasic {
				secret = fixtureClientSecret
			}
			f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, authentication, secret)
			if err != nil {
				t.Fatal(err)
			}
			manager, _ = cognitoSessionTools(t, f)
			if principal, err := manager.ResolveBound(ctx, credentials.Token, browserHash); err != nil || principal.SessionID != credentials.Principal.SessionID {
				t.Fatalf("bound session failed after reopening storage: %v", err)
			}
			if _, err := f.db.ReserveCognitoCodeExchange(ctx, cognito.CodeExchangeReservation{
				OperationID: start.OperationID, DenialID: "bound-restart-denial", Attempt: consumed,
				BrowserBindingHash: browserHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt,
			}); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("bound exchange reservation did not survive reopening storage: %v", err)
			}
		})
	}
}

func TestStableBoundCognitoExchangeRejectsUnboundProofWithoutConsumingIt(t *testing.T) {
	f, browserHash, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	start, consumed := f.consumeAttempt(t)
	if _, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("bound exchange accepted an unbound attempt: %v", err)
	}
	if _, err := f.db.ReserveCognitoCodeExchange(ctx, cognito.CodeExchangeReservation{
		OperationID: start.OperationID, DenialID: "unbound-as-bound", Attempt: consumed,
		BrowserBindingHash: browserHash, OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt,
	}); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("direct bound reservation accepted an unbound attempt: %v", err)
	}
	exchange, err := f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode)
	if err != nil || f.tokenCalls.Load() != 1 {
		t.Fatalf("bound denial consumed the unbound attempt: %v", err)
	}
	manager, protector := cognitoSessionTools(t, f)
	if _, err := exchange.ForBoundSession(protector, f.clock(), browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("bound proof access accepted an unbound exchange: %v", err)
	}
	if credentials, err := manager.IssueBoundCognito(ctx, exchange, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || credentials.Token != "" {
		t.Fatalf("bound issuance accepted an unbound exchange: %v", err)
	}
	if credentials, err := manager.IssueCognito(ctx, exchange); err != nil || credentials.Token == "" {
		t.Fatalf("bound denial consumed the unbound session proof: %v", err)
	}
}

func TestStableBoundCognitoExchangeStorageEnforcesSessionProof(t *testing.T) {
	for _, mode := range []string{"wrong marker", "missing marker", "unbound proof"} {
		t.Run(mode, func(t *testing.T) {
			f, browserHash, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			_, wrongHash := stableCognitoAttemptMarker(12)
			var exchange cognito.CodeExchangeResult
			if mode == "unbound proof" {
				_, exchange = cognitoSessionExchange(t, f)
			} else {
				_, exchange = boundCognitoSessionExchange(t, f, browserHash)
			}
			_, protector := cognitoSessionTools(t, f)
			var proof cognito.VerifiedCodeExchange
			var err error
			if mode == "unbound proof" {
				proof, err = exchange.ForSession(protector, f.clock())
			} else {
				proof, err = exchange.ForBoundSession(protector, f.clock(), browserHash)
			}
			if err != nil {
				t.Fatal(err)
			}
			record, _ := browserSessionRecord("direct-"+mode, "", browserHash, f.clock())
			correct := session.CognitoRecord{Session: record, CandidateAccount: "direct-cognito", Exchange: proof, Protector: protector}
			if mode == "unbound proof" {
				correct.Session.BrowserBindingHash = [32]byte{}
			}
			wrong := correct
			switch mode {
			case "wrong marker":
				wrong.Session.BrowserBindingHash = wrongHash
			case "missing marker":
				wrong.Session.BrowserBindingHash = [32]byte{}
			case "unbound proof":
				wrong.Session.BrowserBindingHash = browserHash
			}
			if principal, err := f.db.CreateCognitoSession(ctx, wrong); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || principal.SessionID != "" {
				t.Fatalf("direct session storage accepted a mismatched proof: %v", err)
			}
			var details cognito.SessionExchangeDetails
			if mode == "unbound proof" {
				details, err = proof.Details(protector, f.clock())
			} else {
				details, err = proof.BoundDetails(protector, f.clock(), browserHash)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.db.reserveCognitoSession(ctx, wrong, details); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("direct session reservation accepted a mismatched proof: %v", err)
			}
			var reserved int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM audit_events WHERE event_id = ?", details.OperationID+".session.attempt").Scan(&reserved); err != nil || reserved != 0 {
				t.Fatalf("wrong marker consumed the correct session proof: count=%d err=%v", reserved, err)
			}
			if principal, err := f.db.CreateCognitoSession(ctx, correct); err != nil || principal.SessionID != correct.Session.SessionID {
				t.Fatalf("mismatched storage request consumed the valid proof: %v", err)
			}
		})
	}
}

func TestStableBoundCognitoExchangeSerializesConcurrentRequests(t *testing.T) {
	f, browserHash, _ := newBrowserSessionFixture(t)
	_, consumed := boundCognitoSessionAttempt(t, f, browserHash)
	const callers = 16
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash)
			if err == nil && result.RefreshToken != fixtureRefresh {
				results <- errors.New("successful exchange returned no verified token")
				return
			}
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
		case errors.Is(err, cognito.ErrCodeExchangeUnavailable):
			denied++
		default:
			t.Fatalf("unexpected concurrent bound exchange result: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
		t.Fatalf("bound exchange was not one-shot: success=%d denied=%d calls=%d", success, denied, f.tokenCalls.Load())
	}
}

func TestStableBoundCognitoExchangeConcurrentSessionProofIsolation(t *testing.T) {
	f, browserHash, _ := newBrowserSessionFixture(t)
	_, wrongHash := stableCognitoAttemptMarker(9)
	_, exchange := boundCognitoSessionExchange(t, f, browserHash)
	manager, _ := cognitoSessionTools(t, f)
	const correctCallers = 8
	const wrongCallers = 8
	results := make(chan error, correctCallers+wrongCallers)
	var wait sync.WaitGroup
	for caller := 0; caller < correctCallers+wrongCallers; caller++ {
		wait.Add(1)
		go func(wrong bool) {
			defer wait.Done()
			hash := browserHash
			if wrong {
				hash = wrongHash
			}
			credentials, err := manager.IssueBoundCognito(context.Background(), exchange, hash)
			if err == nil && (wrong || credentials.Token == "") {
				results <- errors.New("wrong or empty bound session succeeded")
				return
			}
			if wrong && !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				results <- fmt.Errorf("wrong marker returned an unexpected result: %w", err)
				return
			}
			results <- err
		}(caller >= correctCallers)
	}
	wait.Wait()
	close(results)
	success, replay, mismatch := 0, 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, session.ErrCognitoSessionUnavailable):
			replay++
		case errors.Is(err, cognito.ErrCodeExchangeUnavailable):
			mismatch++
		default:
			t.Fatalf("unexpected concurrent session result: %v", err)
		}
	}
	if success != 1 || replay != correctCallers-1 || mismatch != wrongCallers || f.tokenCalls.Load() != 1 {
		t.Fatalf("bound session proof isolation failed: success=%d replay=%d mismatch=%d", success, replay, mismatch)
	}
	var sessions int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ?", browserHash[:]).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("concurrent bound issuance created competing sessions: %d %v", sessions, err)
	}
}

func TestStableBoundCognitoExchangeProviderFailuresNeverReplay(t *testing.T) {
	for _, test := range []struct {
		mode string
		want error
	}{
		{"outage", cognito.ErrCodeExchangeUnknown},
		{"disconnect", cognito.ErrCodeExchangeUnknown},
		{"forged", cognito.ErrInvalidIDToken},
		{"wrong nonce", cognito.ErrInvalidIDToken},
		{"missing refresh", cognito.ErrCodeExchangeUnknown},
	} {
		t.Run(test.mode, func(t *testing.T) {
			f := newCognitoCodeFixture(t, test.mode, cognito.ClientAuthenticationPublic)
			_, browserHash := stableCognitoAttemptMarker(1)
			_, consumed := boundCognitoSessionAttempt(t, f, browserHash)
			result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash)
			if !errors.Is(err, test.want) || result.RefreshToken != "" || result.Identity.Subject != "" || f.tokenCalls.Load() != 1 {
				t.Fatalf("failed provider returned bound credentials: %v", err)
			}
			if strings.Contains(err.Error(), fixtureCode) || strings.Contains(err.Error(), fixtureRefresh) {
				t.Fatal("bound exchange error exposed provider credentials")
			}
			if _, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 1 {
				t.Fatalf("failed bound provider request was replayed: %v", err)
			}
		})
	}
}

func TestStableBoundCognitoExchangeRejectsStaleAndQueuedExpiry(t *testing.T) {
	t.Run("stale verified claims", func(t *testing.T) {
		f := newCognitoCodeFixture(t, "stale", cognito.ClientAuthenticationPublic)
		_, browserHash := stableCognitoAttemptMarker(1)
		_, consumed := boundCognitoSessionAttempt(t, f, browserHash)
		result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash)
		if !errors.Is(err, cognito.ErrCodeExchangeExpired) || result.RefreshToken != "" || f.tokenCalls.Load() != 1 {
			t.Fatalf("stale verified claims returned a bound exchange: %v", err)
		}
		events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
		if err != nil || len(events) != 2 || events[1].Outcome != AuditValidationRejected {
			t.Fatalf("stale claims were not durably rejected: count=%d err=%v", len(events), err)
		}
	})
	t.Run("queued expiry", func(t *testing.T) {
		f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
		_, browserHash := stableCognitoAttemptMarker(1)
		start, consumed := boundCognitoSessionAttempt(t, f, browserHash)
		err := runQueuedDatabaseOperation(t, f.db, func() {
			f.current.Store(start.ExpiresAt.UnixNano())
		}, func() error {
			result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash)
			if result.RefreshToken != "" {
				return errors.New("queued expired exchange returned credentials")
			}
			return err
		})
		if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 0 {
			t.Fatalf("queued expired exchange contacted provider: %v", err)
		}
		events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
		if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied {
			t.Fatalf("queued bound exchange expiry was not audited: count=%d err=%v", len(events), err)
		}
	})
	t.Run("cancelled storage", func(t *testing.T) {
		f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
		_, browserHash := stableCognitoAttemptMarker(1)
		_, consumed := boundCognitoSessionAttempt(t, f, browserHash)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if result, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash); err == nil || result.RefreshToken != "" || f.tokenCalls.Load() != 0 {
			t.Fatal("cancelled bound exchange contacted provider or returned credentials")
		}
		if result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash); err != nil || result.RefreshToken == "" {
			t.Fatalf("cancelled reservation consumed the bound proof: %v", err)
		}
	})
}

func TestStableBoundCognitoExchangeAuditAndCommitFailures(t *testing.T) {
	for _, mode := range []string{"reservation audit", "outcome audit", "reservation commit", "outcome commit"} {
		t.Run(mode, func(t *testing.T) {
			f, browserHash, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			_, consumed := boundCognitoSessionAttempt(t, f, browserHash)
			outcome := "attempted"
			if strings.HasPrefix(mode, "outcome") {
				outcome = "succeeded"
			}
			var script string
			if strings.HasSuffix(mode, "audit") {
				script = `CREATE TRIGGER reject_bound_exchange BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.exchange' AND NEW.outcome = '` + outcome + `'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			} else {
				script = `CREATE TABLE bound_exchange_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_bound_exchange_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.exchange' AND NEW.outcome = '` + outcome + `'
BEGIN INSERT INTO bound_exchange_commit_failure (identity_id) VALUES ('missing-identity'); END;`
			}
			if _, err := f.db.db.Exec(script); err != nil {
				t.Fatal(err)
			}
			result, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash)
			if err == nil || result.RefreshToken != "" || result.Identity.Subject != "" {
				t.Fatalf("failed bound exchange returned credentials: %v", err)
			}
			if strings.HasSuffix(mode, "audit") && !errors.Is(err, cognito.ErrCodeExchangeAudit) {
				t.Fatalf("bound audit failure was hidden: %v", err)
			}
			if strings.HasSuffix(mode, "commit") && !errors.Is(err, cognito.ErrCodeExchangeUnknown) {
				t.Fatalf("bound commit failure was not uncertain: %v", err)
			}
			wantCalls := int64(0)
			if strings.HasPrefix(mode, "outcome") {
				wantCalls = 1
				if _, err := f.exchanger.ExchangeBoundAuthorizationCode(ctx, consumed, fixtureCode, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
					t.Fatalf("uncertain bound exchange was replayed: %v", err)
				}
			}
			if f.tokenCalls.Load() != wantCalls {
				t.Fatalf("unexpected provider calls after %s: %d", mode, f.tokenCalls.Load())
			}
		})
	}
}

type stableCognitoExchangeHookStore struct {
	*Database
	beforeReserve func()
	afterReserve  func()
	beforeFinish  func()
	afterFinish   func()
}

func (s *stableCognitoExchangeHookStore) ReserveCognitoCodeExchange(ctx context.Context, request cognito.CodeExchangeReservation) (time.Time, error) {
	if s.beforeReserve != nil {
		s.beforeReserve()
	}
	when, err := s.Database.ReserveCognitoCodeExchange(ctx, request)
	if err == nil && s.afterReserve != nil {
		s.afterReserve()
	}
	return when, err
}

func (s *stableCognitoExchangeHookStore) FinishCognitoCodeExchange(ctx context.Context, outcome cognito.CodeExchangeOutcome) (time.Time, error) {
	if s.beforeFinish != nil {
		s.beforeFinish()
	}
	when, err := s.Database.FinishCognitoCodeExchange(ctx, outcome)
	if err == nil && s.afterFinish != nil {
		s.afterFinish()
	}
	return when, err
}

func TestStableBoundCognitoExchangeRechecksClockAndDeadline(t *testing.T) {
	for _, mode := range []string{"before reservation", "queued regression", "after reservation regression",
		"regression after decision", "after reservation expiry", "before outcome expiry", "after outcome expiry", "after outcome regression"} {
		t.Run(mode, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			_, browserHash := stableCognitoAttemptMarker(1)
			store := &stableCognitoExchangeHookStore{Database: f.db}
			var err error
			f.manager, err = cognito.NewAttemptManager(store, rand.Reader, f.clock, f.key, f.provider, f.callback)
			if err != nil {
				t.Fatal(err)
			}
			f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
			if err != nil {
				t.Fatal(err)
			}
			start, consumed := boundCognitoSessionAttempt(t, f, browserHash)
			initial := f.clock()
			backward := func() { f.current.Store(initial.Add(-time.Second).UnixNano()) }
			expire := func() { f.current.Store(start.ExpiresAt.UnixNano()) }
			switch mode {
			case "before reservation":
				backward()
			case "queued regression":
				store.beforeReserve = backward
			case "after reservation regression":
				store.afterReserve = backward
			case "regression after decision":
				store.beforeReserve = func() { f.current.Store(initial.Add(20 * time.Second).UnixNano()) }
				store.afterReserve = func() { f.current.Store(initial.Add(10 * time.Second).UnixNano()) }
			case "after reservation expiry":
				store.afterReserve = expire
			case "before outcome expiry":
				store.beforeFinish = expire
			case "after outcome expiry":
				store.afterFinish = expire
			case "after outcome regression":
				store.afterFinish = backward
			}
			result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash)
			if err == nil || result.RefreshToken != "" || result.Identity.Subject != "" {
				t.Fatalf("clock change returned bound credentials: %v", err)
			}
			if mode == "before reservation" || mode == "after reservation regression" || mode == "regression after decision" ||
				mode == "after outcome expiry" || mode == "after outcome regression" {
				if !errors.Is(err, cognito.ErrCodeExchangeUnknown) {
					t.Fatalf("clock change did not report an unknown outcome: %v", err)
				}
			} else if mode == "queued regression" {
				if !errors.Is(err, ErrInvalidAuditEvent) {
					t.Fatalf("queued clock regression was not surfaced: %v", err)
				}
			} else if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("expired bound exchange was not denied: %v", err)
			}
			wantCalls := int64(0)
			if strings.Contains(mode, "outcome") {
				wantCalls = 1
			}
			if f.tokenCalls.Load() != wantCalls {
				t.Fatalf("provider calls after %s: %d", mode, f.tokenCalls.Load())
			}
			f.current.Store(initial.UnixNano())
			store.beforeReserve, store.afterReserve, store.beforeFinish, store.afterFinish = nil, nil, nil, nil
			if mode == "before reservation" || mode == "queued regression" {
				if result, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash); err != nil || result.RefreshToken == "" {
					t.Fatalf("unreserved proof was consumed by clock failure: %v", err)
				}
			} else if _, err := f.exchanger.ExchangeBoundAuthorizationCode(context.Background(), consumed, fixtureCode, browserHash); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("reserved bound proof was replayed after clock failure: %v", err)
			}
		})
	}
}
