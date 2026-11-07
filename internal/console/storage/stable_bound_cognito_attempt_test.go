package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func stableCognitoAttemptMarker(value byte) (string, [32]byte) {
	marker := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{value}, 32))
	return marker, sha256.Sum256([]byte(marker))
}

func TestStableBoundCognitoAttemptIsolationRestartAndRedaction(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	marker, browserHash := stableCognitoAttemptMarker(1)
	_, wrongHash := stableCognitoAttemptMarker(2)
	if _, err := f.manager.StartBoundAuthorization(ctx, [32]byte{}); !errors.Is(err, cognito.ErrInvalidAttemptConfig) {
		t.Fatalf("missing stable marker was accepted for start: %v", err)
	}
	if _, err := f.manager.StartBoundAuthorization(nil, browserHash); !errors.Is(err, cognito.ErrInvalidAttemptConfig) {
		t.Fatalf("nil start context was accepted: %v", err)
	}
	start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	location, err := f.exchanger.BrowserAuthorizationURL(start)
	if err != nil {
		t.Fatalf("bound authorization URL failed: %v", err)
	}
	parsed, err := url.Parse(location)
	if err != nil || parsed.Query().Get("state") != start.State || parsed.Query().Get("nonce") != start.Nonce ||
		parsed.Query().Get("code_challenge") != start.CodeChallenge {
		t.Fatal("bound authorization URL lost its existing parameters")
	}
	cookie, err := cognito.BrowserAuthorizationCookie(start)
	if err != nil || cookie.Value != start.BrowserBinding || cookie.Domain != "" || cookie.Path != "/" ||
		!cookie.Secure || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode {
		t.Fatal("bound attempt did not produce the transient secure cookie")
	}
	for _, secret := range []string{marker, hex.EncodeToString(browserHash[:]), base64.RawURLEncoding.EncodeToString(browserHash[:]), start.BrowserBinding} {
		if strings.Contains(location, secret) {
			t.Fatal("authorization URL exposed a browser binding")
		}
	}

	var storedBinding, encrypted []byte
	if err := f.db.db.QueryRowContext(ctx, `SELECT binding_hash, encrypted_pkce
FROM cognito_authorization_attempts WHERE operation_id = ?`, start.OperationID).Scan(&storedBinding, &encrypted); err != nil {
		t.Fatal(err)
	}
	transientHash := sha256.Sum256([]byte(start.BrowserBinding))
	composite := append([]byte("Kafka3O Cognito stable browser binding v1\x00"), transientHash[:]...)
	composite = append(composite, browserHash[:]...)
	expected := sha256.Sum256(composite)
	if !bytes.Equal(storedBinding, expected[:]) || bytes.Equal(storedBinding, transientHash[:]) || bytes.Equal(storedBinding, browserHash[:]) {
		t.Fatal("stored browser binding was not the domain-separated composite digest")
	}

	otherBinding := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))
	for _, attempt := range []struct {
		name string
		call func() (cognito.ConsumedAuthorization, error)
	}{
		{"wrong stable marker", func() (cognito.ConsumedAuthorization, error) {
			return f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, wrongHash)
		}},
		{"missing stable marker", func() (cognito.ConsumedAuthorization, error) {
			return f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, [32]byte{})
		}},
		{"unbound consumer", func() (cognito.ConsumedAuthorization, error) {
			return f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
		}},
		{"wrong transient cookie", func() (cognito.ConsumedAuthorization, error) {
			return f.manager.ConsumeBoundAuthorization(ctx, start.State, otherBinding, browserHash)
		}},
		{"malformed state", func() (cognito.ConsumedAuthorization, error) {
			return f.manager.ConsumeBoundAuthorization(ctx, "invalid state", start.BrowserBinding, browserHash)
		}},
	} {
		result, err := attempt.call()
		if !errors.Is(err, cognito.ErrInvalidAttempt) || result.CodeVerifier != "" {
			t.Fatalf("%s was accepted: %v", attempt.name, err)
		}
	}
	if _, err := f.manager.ConsumeBoundAuthorization(nil, start.State, start.BrowserBinding, browserHash); !errors.Is(err, cognito.ErrInvalidAttemptConfig) {
		t.Fatalf("nil callback context was accepted: %v", err)
	}
	f.reopen(t)
	consumed, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, cookie.Value, browserHash)
	if err != nil || consumed.OperationID != start.OperationID || len(consumed.CodeVerifier) != 43 {
		t.Fatalf("correct stable marker failed after restart: %v", err)
	}
	challenge := sha256.Sum256([]byte(consumed.CodeVerifier))
	if base64.RawURLEncoding.EncodeToString(challenge[:]) != start.CodeChallenge || bytes.Contains(encrypted, []byte(consumed.CodeVerifier)) {
		t.Fatal("bound PKCE challenge or encryption was incorrect")
	}
	if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, cookie.Value, browserHash); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("bound callback replay was accepted: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 20})
	if err != nil || len(events) != 8 || events[0].Outcome != AuditAttempted ||
		events[6].Outcome != AuditAccepted || events[7].Outcome != AuditDenied {
		t.Fatalf("unexpected bound-attempt audit: count=%d err=%v", len(events), err)
	}
	for _, event := range events[1:6] {
		if event.Outcome != AuditDenied {
			t.Fatal("a rejected browser binding was not audited")
		}
	}
	serialized, err := json.Marshal([]any{start, consumed, events})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{marker, hex.EncodeToString(browserHash[:]), base64.RawURLEncoding.EncodeToString(browserHash[:]),
		start.State, start.Nonce, start.BrowserBinding, start.CodeChallenge, consumed.CodeVerifier} {
		if bytes.Contains(serialized, []byte(secret)) {
			t.Fatal("bound authorization values appeared in serialized output or audit")
		}
		for _, path := range []string{f.path, f.path + "-wal"} {
			data, err := os.ReadFile(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("bound authorization value was stored in plaintext")
			}
		}
	}
}

func TestStableBoundCognitoAttemptDoesNotConsumeUnboundAttempt(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("bound consumer accepted an unbound attempt: %v", err)
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); err != nil {
		t.Fatalf("bound denial consumed the unbound attempt: %v", err)
	}
}

func TestStableBoundCognitoAttemptCannotUseUnboundExchange(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode)
	if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || result.RefreshToken != "" || f.tokenCalls.Load() != 0 {
		t.Fatalf("unbound exchange accepted a stable-bound proof: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
	if err != nil || len(events) != 0 {
		t.Fatalf("rejected proof reserved an exchange: count=%d err=%v", len(events), err)
	}
}

func TestStableBoundCognitoAttemptSerializesConcurrentConsumption(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 24
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
			if err == nil && (result.OperationID != start.OperationID || result.CodeVerifier == "") {
				results <- errors.New("successful consumption returned no verifier")
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
		case errors.Is(err, cognito.ErrInvalidAttempt):
			denied++
		default:
			t.Fatalf("unexpected concurrent callback error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 {
		t.Fatalf("bound single-use outcomes: successes=%d denials=%d", success, denied)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.callback", Limit: 100})
	if err != nil || len(events) != callers {
		t.Fatalf("concurrent bound callbacks were not audited: count=%d err=%v", len(events), err)
	}
}

func TestStableBoundCognitoAttemptAuthenticatesBindingAndCiphertext(t *testing.T) {
	for _, mode := range []string{"substituted binding", "ciphertext", "nonce", "expiry", "operation"} {
		t.Run(mode, func(t *testing.T) {
			f := newCognitoAttemptFixture(t)
			ctx := context.Background()
			_, browserHash := stableCognitoAttemptMarker(1)
			_, wrongHash := stableCognitoAttemptMarker(2)
			start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
			if err != nil {
				t.Fatal(err)
			}
			consumerHash := browserHash
			expectedOperation := start.OperationID
			switch mode {
			case "substituted binding":
				transientHash := sha256.Sum256([]byte(start.BrowserBinding))
				composite := append([]byte("Kafka3O Cognito stable browser binding v1\x00"), transientHash[:]...)
				composite = append(composite, wrongHash[:]...)
				substitute := sha256.Sum256(composite)
				_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET binding_hash = ? WHERE operation_id = ?", substitute[:], start.OperationID)
				consumerHash = wrongHash
			case "ciphertext":
				var ciphertext []byte
				err = f.db.db.QueryRowContext(ctx, "SELECT encrypted_pkce FROM cognito_authorization_attempts WHERE operation_id = ?", start.OperationID).Scan(&ciphertext)
				if err == nil {
					ciphertext[len(ciphertext)-1] ^= 1
					_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET encrypted_pkce = ? WHERE operation_id = ?", ciphertext, start.OperationID)
				}
			case "nonce":
				nonce := sha256.Sum256([]byte("substituted nonce"))
				_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET nonce_hash = ? WHERE operation_id = ?", nonce[:], start.OperationID)
			case "expiry":
				_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET expires_at_ns = ? WHERE operation_id = ?",
					start.ExpiresAt.Add(time.Hour).UnixNano(), start.OperationID)
				f.current.Store(start.ExpiresAt.Add(time.Second).UnixNano())
			case "operation":
				expectedOperation = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
				_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET operation_id = ? WHERE operation_id = ?", expectedOperation, start.OperationID)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, consumerHash)
			if !errors.Is(err, cognito.ErrAttemptEncryption) || result.CodeVerifier != "" || result.OperationID != expectedOperation {
				t.Fatalf("tampered bound attempt was accepted: %v", err)
			}
			if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); !errors.Is(err, cognito.ErrInvalidAttempt) {
				t.Fatalf("tampered attempt was retried: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.callback", Limit: 10})
			if err != nil || len(events) != 3 || events[0].Outcome != AuditAccepted || events[1].Outcome != AuditFailed ||
				events[1].OperationID != expectedOperation || events[2].Outcome != AuditDenied {
				t.Fatalf("bound decryption failure was not audited: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestStableBoundCognitoAttemptAuditFailureRollsBack(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	_, wrongHash := stableCognitoAttemptMarker(2)
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_bound_start BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.authorize' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	failed, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err == nil || failed.OperationID == "" || failed.State != "" || failed.BrowserBinding != "" {
		t.Fatal("bound start returned credentials without audit")
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unaudited bound attempt persisted: count=%d err=%v", count, err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_bound_start"); err != nil {
		t.Fatal(err)
	}
	start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_bound_callback BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.callback' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	for _, marker := range [][32]byte{wrongHash, {}, browserHash} {
		result, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, marker)
		if err == nil || errors.Is(err, cognito.ErrInvalidAttempt) || result.CodeVerifier != "" {
			t.Fatalf("bound callback hid an audit failure: %v", err)
		}
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_bound_callback"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); err != nil {
		t.Fatalf("failed callback audit consumed the attempt: %v", err)
	}
}

func TestStableBoundCognitoAttemptFailureAuditAndCancelledStorage(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if result, err := f.manager.StartBoundAuthorization(cancelled, browserHash); err == nil || result.State != "" {
		t.Fatal("cancelled storage exposed bound start credentials")
	}
	if result, err := f.manager.ConsumeBoundAuthorization(cancelled, start.State, start.BrowserBinding, browserHash); err == nil || result.CodeVerifier != "" {
		t.Fatal("cancelled storage exposed bound callback credentials")
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_bound_failure BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.callback' AND NEW.outcome = 'failed'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	wrongKey, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, bytes.Repeat([]byte{9}, 32), f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	result, err := wrongKey.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
	if !errors.Is(err, cognito.ErrAttemptEncryption) || !errors.Is(err, cognito.ErrAttemptAudit) || result.CodeVerifier != "" {
		t.Fatalf("bound decryption audit failure was hidden: %v", err)
	}
	if strings.Contains(err.Error(), start.State) || strings.Contains(err.Error(), start.BrowserBinding) {
		t.Fatal("bound authorization value appeared in failure")
	}
	if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("failed bound decryption was retried: %v", err)
	}
}

func TestStableBoundCognitoAttemptUnknownCommitWithholdsCredentials(t *testing.T) {
	for _, operation := range []string{"start", "consume"} {
		t.Run(operation, func(t *testing.T) {
			f := newCognitoAttemptFixture(t)
			ctx := context.Background()
			_, browserHash := stableCognitoAttemptMarker(1)
			var start cognito.AuthorizationStart
			var err error
			if operation == "consume" {
				start, err = f.manager.StartBoundAuthorization(ctx, browserHash)
				if err != nil {
					t.Fatal(err)
				}
			}
			trigger := "AFTER INSERT"
			if operation == "consume" {
				trigger = "AFTER DELETE"
			}
			if _, err := f.db.db.ExecContext(ctx, `CREATE TABLE bound_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_bound_commit `+trigger+` ON cognito_authorization_attempts
BEGIN INSERT INTO bound_commit_failure (identity_id) VALUES ('missing-identity'); END;`); err != nil {
				t.Fatal(err)
			}
			if operation == "start" {
				start, err = f.manager.StartBoundAuthorization(ctx, browserHash)
				if !errors.Is(err, cognito.ErrAttemptUnknown) || start.OperationID == "" || start.State != "" || start.BrowserBinding != "" {
					t.Fatalf("uncertain bound start returned credentials: %v", err)
				}
			} else {
				consumed, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
				if !errors.Is(err, cognito.ErrAttemptUnknown) || consumed.OperationID != start.OperationID || consumed.CodeVerifier != "" {
					t.Fatalf("uncertain bound callback returned credentials: %v", err)
				}
			}
			f.reopen(t)
			var count int
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if operation == "consume" {
				want = 1
			}
			if count != want {
				t.Fatalf("failed bound commit left a partial outcome: count=%d", count)
			}
		})
	}
}

func TestStableBoundCognitoAttemptQueuedExpiryAndClockRegression(t *testing.T) {
	for _, operation := range []string{"start", "consume"} {
		for _, mode := range []string{"expiry", "clock regression"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				f := newCognitoAttemptFixture(t)
				ctx := context.Background()
				_, browserHash := stableCognitoAttemptMarker(1)
				initial := f.clock()
				start, err := f.manager.StartBoundAuthorization(ctx, browserHash)
				if err != nil {
					t.Fatal(err)
				}
				err = runQueuedDatabaseOperation(t, f.db, func() {
					if mode == "expiry" {
						f.current.Store(initial.Add(cognito.AttemptLifetime).UnixNano())
					} else {
						f.current.Store(initial.Add(-time.Second).UnixNano())
					}
				}, func() error {
					if operation == "start" {
						result, err := f.manager.StartBoundAuthorization(ctx, browserHash)
						if result.State != "" || result.BrowserBinding != "" {
							return errors.New("expired or regressed start returned credentials")
						}
						return err
					}
					result, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
					if result.CodeVerifier != "" {
						return errors.New("expired or regressed callback returned a verifier")
					}
					return err
				})
				if mode == "expiry" && !errors.Is(err, cognito.ErrInvalidAttempt) {
					t.Fatalf("queued expired bound %s was not denied: %v", operation, err)
				}
				if mode == "clock regression" && (err == nil || errors.Is(err, cognito.ErrInvalidAttempt)) {
					t.Fatalf("queued regressed bound %s was not an explicit failure: %v", operation, err)
				}
				if mode == "clock regression" {
					f.current.Store(initial.UnixNano())
					if _, err := f.manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); err != nil {
						t.Fatalf("regressed operation consumed a live attempt: %v", err)
					}
				}
			})
		}
	}
}

type stableCognitoAttemptHookStore struct {
	*Database
	beforeCreate   func()
	afterCreate    func()
	beforeConsume  func()
	afterConsume   func()
	invalidSuccess bool
}

func (s *stableCognitoAttemptHookStore) CreateCognitoAttempt(ctx context.Context, attempt cognito.AttemptRecord) (time.Time, error) {
	if s.beforeCreate != nil {
		s.beforeCreate()
	}
	createdAt, err := s.Database.CreateCognitoAttempt(ctx, attempt)
	if err == nil && s.afterCreate != nil {
		s.afterCreate()
	}
	return createdAt, err
}

func (s *stableCognitoAttemptHookStore) ConsumeCognitoAttempt(ctx context.Context, request cognito.AttemptConsumption) (cognito.AttemptRecord, error) {
	if s.beforeConsume != nil {
		s.beforeConsume()
	}
	if !request.Valid && s.invalidSuccess {
		return cognito.AttemptRecord{OperationID: request.EventID}, nil
	}
	record, err := s.Database.ConsumeCognitoAttempt(ctx, request)
	if err == nil && s.afterConsume != nil {
		s.afterConsume()
	}
	return record, err
}

func TestStableBoundCognitoAttemptPostCommitTimeChangesWithholdCredentials(t *testing.T) {
	for _, operation := range []string{"start", "consume"} {
		for _, mode := range []string{"expiry", "clock regression", "regression after decision"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				f := newCognitoAttemptFixture(t)
				ctx := context.Background()
				_, browserHash := stableCognitoAttemptMarker(1)
				initial := f.clock()
				store := &stableCognitoAttemptHookStore{Database: f.db}
				manager, err := cognito.NewAttemptManager(store, rand.Reader, f.clock, f.key, f.provider, f.callback)
				if err != nil {
					t.Fatal(err)
				}
				changeClock := func() {
					if mode == "expiry" {
						f.current.Store(initial.Add(cognito.AttemptLifetime).UnixNano())
					} else if mode == "regression after decision" {
						f.current.Store(initial.Add(10 * time.Second).UnixNano())
					} else {
						f.current.Store(initial.Add(-time.Second).UnixNano())
					}
				}
				forwardClock := func() {
					f.current.Store(initial.Add(20 * time.Second).UnixNano())
				}
				if operation == "start" {
					if mode == "regression after decision" {
						store.beforeCreate = forwardClock
					}
					store.afterCreate = changeClock
					result, err := manager.StartBoundAuthorization(ctx, browserHash)
					if err != cognito.ErrAttemptUnknown || result.OperationID == "" || result.State != "" || result.BrowserBinding != "" {
						t.Fatalf("post-commit clock change exposed bound start credentials: %v", err)
					}
				} else {
					start, err := manager.StartBoundAuthorization(ctx, browserHash)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "regression after decision" {
						store.beforeConsume = forwardClock
					}
					store.afterConsume = changeClock
					result, err := manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash)
					if err != cognito.ErrAttemptUnknown || result.OperationID != start.OperationID || result.CodeVerifier != "" {
						t.Fatalf("post-commit clock change exposed bound callback credentials: %v", err)
					}
					f.current.Store(initial.UnixNano())
					store.beforeConsume = nil
					store.afterConsume = nil
					if _, err := manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); !errors.Is(err, cognito.ErrInvalidAttempt) {
						t.Fatalf("post-commit unknown callback could be replayed: %v", err)
					}
				}

			})
		}
	}
}

func TestStableBoundCognitoAttemptRejectsNonconformingStore(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	_, browserHash := stableCognitoAttemptMarker(1)
	store := &stableCognitoAttemptHookStore{Database: f.db, invalidSuccess: true}
	manager, err := cognito.NewAttemptManager(store, rand.Reader, f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	start, err := manager.StartBoundAuthorization(ctx, browserHash)
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, [32]byte{})
	if err != cognito.ErrAttemptUnknown || result.CodeVerifier != "" {
		t.Fatalf("nonconforming store accepted an invalid marker: %v", err)
	}
	if _, err := manager.ConsumeBoundAuthorization(ctx, start.State, start.BrowserBinding, browserHash); err != nil {
		t.Fatalf("invalid marker consumed the correct attempt: %v", err)
	}
}
