package storage

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type cognitoAttemptFixture struct {
	db       *Database
	manager  *cognito.AttemptManager
	current  atomic.Int64
	key      []byte
	path     string
	provider cognito.ProviderConfig
	callback string
}

func newCognitoAttemptFixture(t *testing.T) *cognitoAttemptFixture {
	t.Helper()
	f := &cognitoAttemptFixture{
		path:     filepath.Join(t.TempDir(), "cognito-attempts.sqlite"),
		key:      bytes.Repeat([]byte{7}, 32),
		provider: cognito.ProviderConfig{Issuer: "https://issuer.example/pool", ClientID: "test-client", AuthorizationURL: "https://login.example/authorize", TokenURL: "https://login.example/token", JWKSURL: "https://issuer.example/jwks"},
		callback: "https://console.example/callback",
	}
	f.current.Store(time.Now().UTC().Add(time.Second).UnixNano())
	var err error
	f.db, err = OpenDatabase(context.Background(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.db.now = f.clock
	f.manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.db.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *cognitoAttemptFixture) clock() time.Time {
	return time.Unix(0, f.current.Load()).UTC()
}

func (f *cognitoAttemptFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.db, err = OpenDatabase(context.Background(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.db.now = f.clock
	f.manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCognitoAttemptPersistsEncryptedAndConsumesOnce(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil || start.OperationID == "" || !start.ExpiresAt.Equal(f.clock().Add(cognito.AttemptLifetime)) {
		t.Fatalf("start failed: %v", err)
	}
	if start.State == start.Nonce || start.State == start.BrowserBinding || start.Nonce == start.BrowserBinding {
		t.Fatal("authorization values were not independent")
	}
	var state, binding, nonce, encrypted []byte
	if err := f.db.db.QueryRowContext(ctx, `SELECT state_hash, binding_hash, nonce_hash, encrypted_pkce
FROM cognito_authorization_attempts WHERE operation_id = ?`, start.OperationID).Scan(&state, &binding, &nonce, &encrypted); err != nil {
		t.Fatal(err)
	}
	stateHash, bindingHash, nonceHash := sha256.Sum256([]byte(start.State)), sha256.Sum256([]byte(start.BrowserBinding)), sha256.Sum256([]byte(start.Nonce))
	if !bytes.Equal(state, stateHash[:]) || !bytes.Equal(binding, bindingHash[:]) || !bytes.Equal(nonce, nonceHash[:]) ||
		len(encrypted) != cognito.EncryptedVerifierSize {
		t.Fatal("stored authorization values were not hashed or encrypted")
	}
	f.reopen(t)
	wrongBinding := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))
	if _, err := f.manager.ConsumeAuthorization(ctx, start.State, wrongBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("wrong browser was accepted: %v", err)
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, "malformed state", start.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("malformed state was accepted: %v", err)
	}
	consumed, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
	if err != nil || consumed.OperationID != start.OperationID || consumed.NonceHash != nonceHash || len(consumed.CodeVerifier) != 43 {
		t.Fatalf("valid persisted attempt failed: %v", err)
	}
	challenge := sha256.Sum256([]byte(consumed.CodeVerifier))
	if base64.RawURLEncoding.EncodeToString(challenge[:]) != start.CodeChallenge || bytes.Contains(encrypted, []byte(consumed.CodeVerifier)) {
		t.Fatal("PKCE challenge or encrypted verifier was incorrect")
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("replayed callback was accepted: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 20})
	if err != nil || len(events) != 5 || events[0].Outcome != AuditAttempted ||
		events[1].Outcome != AuditDenied || events[2].Outcome != AuditDenied ||
		events[3].Outcome != AuditAccepted || events[3].OperationID != start.OperationID ||
		events[4].Outcome != AuditDenied {
		t.Fatalf("unexpected authorization audit: count=%d err=%v", len(events), err)
	}
	serialized, err := json.Marshal([]any{start, consumed, events})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{start.State, start.Nonce, start.BrowserBinding, start.CodeChallenge, consumed.CodeVerifier} {
		if bytes.Contains(serialized, []byte(secret)) {
			t.Fatal("authorization values appeared in serialized results or audit")
		}
		for _, path := range []string{f.path, f.path + "-wal"} {
			data, err := os.ReadFile(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("authorization values were stored in plaintext")
			}
		}
	}
}

func TestCognitoAttemptExpiryAndQueuedOperations(t *testing.T) {
	for _, operation := range []string{"start", "consume"} {
		t.Run(operation, func(t *testing.T) {
			f := newCognitoAttemptFixture(t)
			ctx := context.Background()
			initial := f.clock()
			start, err := f.manager.StartAuthorization(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = runQueuedDatabaseOperation(t, f.db, func() {
				f.current.Store(initial.Add(cognito.AttemptLifetime).UnixNano())
			}, func() error {
				if operation == "start" {
					_, err := f.manager.StartAuthorization(ctx)
					return err
				}
				_, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
				return err
			})
			if !errors.Is(err, cognito.ErrInvalidAttempt) {
				t.Fatalf("queued expired %s was accepted: %v", operation, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied ||
				!events[1].OccurredAt.Equal(initial.Add(cognito.AttemptLifetime)) {
				t.Fatalf("expired request was not audited at decision time: %v", err)
			}
		})
	}
}

func TestCognitoAttemptRejectsMismatchedAndMalformedBindings(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	first, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		state, binding string
	}{
		{first.State, second.BrowserBinding},
		{second.State, first.BrowserBinding},
		{first.State, "invalid binding"},
		{strings.Repeat("x", 44), first.BrowserBinding},
		{strings.Repeat("!", 43), first.BrowserBinding},
	} {
		result, err := f.manager.ConsumeAuthorization(ctx, tt.state, tt.binding)
		if !errors.Is(err, cognito.ErrInvalidAttempt) || result.CodeVerifier != "" {
			t.Fatalf("mismatched or malformed binding was accepted: %v", err)
		}
	}
	for _, start := range []cognito.AuthorizationStart{first, second} {
		if _, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); err != nil {
			t.Fatalf("denied callback consumed another browser's attempt: %v", err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.callback", Limit: 20})
	if err != nil || len(events) != 7 {
		t.Fatalf("callback decisions not audited: count=%d err=%v", len(events), err)
	}
	for _, event := range events[:5] {
		if event.Outcome != AuditDenied {
			t.Fatal("invalid callback did not produce a denial audit")
		}
	}
}

func TestCognitoAttemptLimitIsBoundedAndExpires(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	tx, err := f.db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < cognito.MaxActiveAttempts; i++ {
		state := sha256.Sum256([]byte(fmt.Sprintf("synthetic-state-%d", i)))
		binding, nonce := sha256.Sum256([]byte("binding")), sha256.Sum256([]byte("nonce"))
		_, err := tx.ExecContext(ctx, `INSERT INTO cognito_authorization_attempts
(operation_id, state_hash, binding_hash, nonce_hash, encrypted_pkce, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("seed-%d", i), state[:], binding[:], nonce[:],
			make([]byte, cognito.EncryptedVerifierSize), f.clock().UnixNano(), f.clock().Add(cognito.AttemptLifetime).UnixNano())
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	denied, err := f.manager.StartAuthorization(ctx)
	if !errors.Is(err, cognito.ErrAttemptLimit) || denied.OperationID == "" || denied.State != "" {
		t.Fatalf("attempt capacity was not enforced: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.authorize", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied || events[0].OperationID != denied.OperationID {
		t.Fatalf("capacity denial was not audited: count=%d err=%v", len(events), err)
	}
	f.current.Add(int64(cognito.AttemptLifetime))
	if _, err := f.manager.StartAuthorization(ctx); err != nil {
		t.Fatalf("expired attempts did not release capacity: %v", err)
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("unexpected live attempt count=%d err=%v", count, err)
	}
}

func TestCognitoAttemptSerializesConcurrentConsumption(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
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
			result, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
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
		t.Fatalf("single-use outcomes: successes=%d denials=%d", success, denied)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.callback", Limit: 100})
	if err != nil || len(events) != callers {
		t.Fatalf("concurrent callbacks were not audited: count=%d err=%v", len(events), err)
	}
}

func TestCognitoAttemptFailsClosedOnWrongKeyAndTampering(t *testing.T) {
	for _, mode := range []string{"wrong key", "provider binding", "callback binding", "ciphertext", "nonce binding", "swapped ciphertext"} {
		t.Run(mode, func(t *testing.T) {
			f := newCognitoAttemptFixture(t)
			ctx := context.Background()
			start, err := f.manager.StartAuthorization(ctx)
			if err != nil {
				t.Fatal(err)
			}
			manager := f.manager
			switch mode {
			case "wrong key":
				manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, bytes.Repeat([]byte{8}, 32), f.provider, f.callback)
			case "provider binding":
				other := f.provider
				other.TokenURL = "https://other.example/token"
				manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, other, f.callback)
			case "callback binding":
				manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other")
			case "ciphertext":
				var ciphertext []byte
				err = f.db.db.QueryRowContext(ctx, "SELECT encrypted_pkce FROM cognito_authorization_attempts WHERE operation_id = ?", start.OperationID).Scan(&ciphertext)
				if err == nil {
					ciphertext[len(ciphertext)-1] ^= 1
					_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET encrypted_pkce = ? WHERE operation_id = ?", ciphertext, start.OperationID)
				}
			case "nonce binding":
				nonce := sha256.Sum256([]byte("substituted nonce"))
				_, err = f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET nonce_hash = ? WHERE operation_id = ?", nonce[:], start.OperationID)
			case "swapped ciphertext":
				var another cognito.AuthorizationStart
				another, err = f.manager.StartAuthorization(ctx)
				if err == nil {
					_, err = f.db.db.ExecContext(ctx, `UPDATE cognito_authorization_attempts SET encrypted_pkce =
(SELECT encrypted_pkce FROM cognito_authorization_attempts WHERE operation_id = ?) WHERE operation_id = ?`, another.OperationID, start.OperationID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
			if !errors.Is(err, cognito.ErrAttemptEncryption) || result.OperationID != start.OperationID || result.CodeVerifier != "" {
				t.Fatalf("wrong key or tampering was accepted: %v", err)
			}
			if _, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
				t.Fatalf("failed decryption was retried: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.callback", Limit: 10})
			if err != nil || len(events) != 3 || events[0].Outcome != AuditAccepted ||
				events[1].Outcome != AuditFailed || events[1].OperationID != start.OperationID || events[2].Outcome != AuditDenied {
				t.Fatalf("decryption failure was not audited: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoLegacyUnboundAttemptFailsClosedAfterUpgrade(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var state, binding, nonce []byte
	if err := f.db.db.QueryRowContext(ctx, `SELECT state_hash, binding_hash, nonce_hash
FROM cognito_authorization_attempts WHERE operation_id = ?`, start.OperationID).Scan(&state, &binding, &nonce); err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(f.key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	iv := bytes.Repeat([]byte{1}, aead.NonceSize())
	additional := append(append(append([]byte{}, state...), binding...), nonce...)
	verifier := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	legacy := aead.Seal(append([]byte{}, iv...), iv, []byte(verifier), additional)
	if _, err := f.db.db.ExecContext(ctx, "UPDATE cognito_authorization_attempts SET encrypted_pkce = ? WHERE operation_id = ?", legacy, start.OperationID); err != nil {
		t.Fatal(err)
	}
	result, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
	if !errors.Is(err, cognito.ErrAttemptEncryption) || result.CodeVerifier != "" {
		t.Fatalf("legacy unbound attempt was accepted: %v", err)
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); !errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("legacy attempt could be replayed: %v", err)
	}
}

func TestCognitoAttemptAuditFailureRollsBack(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_start BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.authorize' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	failed, err := f.manager.StartAuthorization(ctx)
	if err == nil || failed.OperationID == "" || failed.State != "" {
		t.Fatal("authorization start succeeded without audit")
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unaudited attempt persisted: count=%d err=%v", count, err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_cognito_start"); err != nil {
		t.Fatal(err)
	}
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_callback BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.callback' BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	result, err := f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
	if err == nil || result.CodeVerifier != "" {
		t.Fatal("authorization consumption succeeded without audit")
	}
	if _, err := f.manager.ConsumeAuthorization(ctx, "invalid", "invalid"); err == nil || errors.Is(err, cognito.ErrInvalidAttempt) {
		t.Fatalf("denial audit failure was hidden: %v", err)
	}
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_authorization_attempts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed consumption did not roll back: count=%d err=%v", count, err)
	}
}

func TestCognitoAttemptUnknownCommitNeverReturnsCredentials(t *testing.T) {
	for _, operation := range []string{"start", "consume"} {
		t.Run(operation, func(t *testing.T) {
			f := newCognitoAttemptFixture(t)
			ctx := context.Background()
			var start cognito.AuthorizationStart
			var err error
			if operation == "consume" {
				start, err = f.manager.StartAuthorization(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			trigger := "AFTER INSERT"
			if operation == "consume" {
				trigger = "AFTER DELETE"
			}
			_, err = f.db.db.ExecContext(ctx, `CREATE TABLE cognito_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_cognito_commit `+trigger+` ON cognito_authorization_attempts
BEGIN INSERT INTO cognito_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "start" {
				start, err = f.manager.StartAuthorization(ctx)
				if !errors.Is(err, cognito.ErrAttemptUnknown) || start.OperationID == "" || start.State != "" {
					t.Fatalf("uncertain start returned credentials: %v", err)
				}
			} else {
				var consumed cognito.ConsumedAuthorization
				consumed, err = f.manager.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
				if !errors.Is(err, cognito.ErrAttemptUnknown) || consumed.OperationID != start.OperationID || consumed.CodeVerifier != "" {
					t.Fatalf("uncertain consumption returned credentials: %v", err)
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
				t.Fatalf("failed test commit left a partial outcome: count=%d", count)
			}
		})
	}
}

func TestCognitoAttemptFailureAuditAndRandomnessErrors(t *testing.T) {
	f := newCognitoAttemptFixture(t)
	ctx := context.Background()
	for _, key := range [][]byte{nil, bytes.Repeat([]byte{0}, 32), bytes.Repeat([]byte{1}, 31)} {
		if _, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, key, f.provider, f.callback); !errors.Is(err, cognito.ErrInvalidAttemptConfig) {
			t.Fatalf("invalid encryption key accepted: %v", err)
		}
	}
	manager, err := cognito.NewAttemptManager(f.db, io.LimitReader(rand.Reader, 0), f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartAuthorization(ctx); err == nil {
		t.Fatal("failed randomness was hidden")
	}
	repeated, err := cognito.NewAttemptManager(f.db, bytes.NewReader(make([]byte, 156)), f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repeated.StartAuthorization(ctx); !errors.Is(err, cognito.ErrInvalidAttemptConfig) {
		t.Fatalf("repeated authorization values were accepted: %v", err)
	}
	start, err := f.manager.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_failure BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.callback' AND NEW.outcome = 'failed'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	wrongKey, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, bytes.Repeat([]byte{9}, 32), f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	result, err := wrongKey.ConsumeAuthorization(ctx, start.State, start.BrowserBinding)
	if !errors.Is(err, cognito.ErrAttemptEncryption) || !errors.Is(err, cognito.ErrAttemptAudit) || result.CodeVerifier != "" {
		t.Fatalf("decryption audit failure was hidden: %v", err)
	}
	if strings.Contains(err.Error(), start.State) || strings.Contains(err.Error(), start.BrowserBinding) {
		t.Fatal("authorization value appeared in failure")
	}
}

func TestCognitoVersionSixMigrationPreservesLocalState(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, "DROP TABLE console_browser_sessions; ALTER TABLE console_sessions DROP COLUMN browser_binding_hash; DROP TABLE cognito_group_bindings; DROP TABLE cognito_session_credentials; DROP TABLE cognito_identities; ALTER TABLE console_sessions DROP COLUMN auth_source; DROP TABLE cognito_authorization_attempts; "+dropSchemaAfterVersionTen+"PRAGMA user_version = 6"); err != nil {
		t.Fatal(err)
	}
	path := f.db.path
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := manager.Resolve(ctx, f.adminRun.Token)
	if err != nil || principal.AccountID != f.admin.ID {
		t.Fatalf("version-six migration lost the administrator session: %v", err)
	}
	var version, tables int
	if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 12 {
		t.Fatalf("version-six migration did not advance: version=%d err=%v", version, err)
	}
	if err := db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'cognito_authorization_attempts'").Scan(&tables); err != nil || tables != 1 {
		t.Fatalf("version-six migration did not create attempt storage: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil || len(events) == 0 {
		t.Fatalf("version-six migration lost audit history: count=%d err=%v", len(events), err)
	}
}
