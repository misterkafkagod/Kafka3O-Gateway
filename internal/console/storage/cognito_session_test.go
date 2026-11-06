package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func cognitoSessionTools(t *testing.T, f *cognitoCodeFixture) (*session.Manager, *cognito.RefreshTokenProtector) {
	t.Helper()
	protector, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, protector)
	if err != nil {
		t.Fatal(err)
	}
	return manager, protector
}

func cognitoSessionExchange(t *testing.T, f *cognitoCodeFixture) (cognito.AuthorizationStart, cognito.CodeExchangeResult) {
	t.Helper()
	start, consumed := f.consumeAttempt(t)
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
	if err != nil {
		t.Fatal(err)
	}
	return start, result
}

func TestCognitoSessionPersistsEncryptedAndFailsClosedWhenStale(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	start, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil || credentials.Principal.AccountID == "" || credentials.Principal.SessionID == "" ||
		credentials.Principal.Source != session.SourceCognito || credentials.Principal.MustChangePassword ||
		credentials.Token == "" || credentials.CSRFToken == "" || credentials.Token == credentials.CSRFToken {
		t.Fatalf("Cognito session was not issued correctly: %v", err)
	}
	var issuer, subject, operationID, audience, groups string
	var nonceHash, encrypted, storedToken, storedCSRF []byte
	var generation, freshUntil int64
	if err := f.db.db.QueryRowContext(ctx, `SELECT cognito_identities.issuer, cognito_identities.subject,
cognito_session_credentials.exchange_operation_id, cognito_session_credentials.original_nonce_hash,
cognito_session_credentials.audience_json, cognito_session_credentials.groups_json,
cognito_session_credentials.refresh_generation, cognito_session_credentials.encrypted_refresh_token,
cognito_session_credentials.claims_fresh_until_ns, console_sessions.token_hash, console_sessions.csrf_hash
FROM cognito_session_credentials JOIN cognito_identities ON cognito_identities.identity_id = cognito_session_credentials.identity_id
JOIN console_sessions ON console_sessions.id = cognito_session_credentials.session_id
WHERE console_sessions.id = ?`, credentials.Principal.SessionID).Scan(
		&issuer, &subject, &operationID, &nonceHash, &audience, &groups, &generation, &encrypted, &freshUntil, &storedToken, &storedCSRF,
	); err != nil {
		t.Fatal(err)
	}
	wantNonce := sha256.Sum256([]byte(start.Nonce))
	tokenBytes, err := base64.RawURLEncoding.DecodeString(credentials.Token)
	if err != nil {
		t.Fatal(err)
	}
	csrfBytes, err := base64.RawURLEncoding.DecodeString(credentials.CSRFToken)
	if err != nil {
		t.Fatal(err)
	}
	tokenHash, csrfHash := sha256.Sum256(tokenBytes), sha256.Sum256(csrfBytes)
	if issuer != f.provider.Issuer || subject != "opaque|synthetic-subject" || operationID != start.OperationID ||
		!bytes.Equal(nonceHash, wantNonce[:]) || !bytes.Equal(storedToken, tokenHash[:]) ||
		!bytes.Equal(storedCSRF, csrfHash[:]) || generation != 1 || bytes.Contains(encrypted, []byte(fixtureRefresh)) {
		t.Fatal("Cognito identity, session digests or encrypted token were stored incorrectly")
	}
	var audienceValues, groupValues []string
	if json.Unmarshal([]byte(audience), &audienceValues) != nil || json.Unmarshal([]byte(groups), &groupValues) != nil ||
		!reflect.DeepEqual(audienceValues, []string{f.provider.ClientID}) || !reflect.DeepEqual(groupValues, []string{"unmapped-group"}) {
		t.Fatal("verified Cognito claim snapshot was not stored")
	}
	binding := cognito.RefreshTokenBinding{SessionID: credentials.Principal.SessionID, IdentityID: credentials.Principal.AccountID, Generation: generation}
	if token, err := protector.Open(binding, encrypted); err != nil || token != fixtureRefresh {
		t.Fatalf("stored refresh token could not be decrypted: %v", err)
	}
	var grants, localAccounts int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE account_id = ?", credentials.Principal.AccountID).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM local_accounts WHERE identity_id = ?", credentials.Principal.AccountID).Scan(&localAccounts); err != nil {
		t.Fatal(err)
	}
	if grants != 0 || localAccounts != 0 {
		t.Fatal("Cognito identity received a direct grant or local-account link")
	}
	if replay, err := manager.IssueCognito(ctx, exchange); !errors.Is(err, session.ErrCognitoSessionUnavailable) || replay.Token != "" {
		t.Fatalf("one-shot exchange issued a second session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.session", Limit: 10})
	if err != nil || len(events) != 3 || events[0].Outcome != AuditAttempted ||
		events[1].Outcome != AuditSucceeded || events[1].ActorID != credentials.Principal.AccountID ||
		events[1].Source != AuditSourceCognito || events[2].Outcome != AuditDenied {
		t.Fatalf("Cognito session creation and replay were not audited: count=%d err=%v", len(events), err)
	}
	for _, path := range []string{f.path, f.path + "-wal"} {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken, start.Nonce, start.State, fixtureCode} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("a provider or session credential was stored as plaintext")
			}
		}
	}
	f.reopen(t)
	manager, protector = cognitoSessionTools(t, f)
	if token, err := protector.Open(binding, encrypted); err != nil || token != fixtureRefresh {
		t.Fatalf("stored refresh token did not survive restart: %v", err)
	}
	f.current.Store(freshUntil - 1)
	principal, err := manager.Resolve(ctx, credentials.Token)
	if err != nil || principal.AccountID != credentials.Principal.AccountID || principal.Source != session.SourceCognito {
		t.Fatalf("fresh persisted Cognito session was unavailable: %v", err)
	}
	f.current.Store(freshUntil)
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("stale Cognito claims were accepted: %v", err)
	}
	if err := manager.Revoke(ctx, credentials.Token); err != nil {
		t.Fatalf("stale Cognito session could not be locally revoked: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("logout retained Cognito refresh-token material: count=%d err=%v", retained, err)
	}
	logouts, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
	if err != nil || len(logouts) != 1 || logouts[0].Source != AuditSourceCognito || logouts[0].ActorID != credentials.Principal.AccountID {
		t.Fatalf("Cognito logout was not audited: count=%d err=%v", len(logouts), err)
	}
}

func TestCognitoSessionsReuseOnlyTheVerifiedIdentityAndRespectDisablement(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	manager, _ := cognitoSessionTools(t, f)
	_, firstExchange := cognitoSessionExchange(t, f)
	first, err := manager.IssueCognito(ctx, firstExchange)
	if err != nil {
		t.Fatal(err)
	}
	_, secondExchange := cognitoSessionExchange(t, f)
	second, err := manager.IssueCognito(ctx, secondExchange)
	if err != nil || second.Principal.AccountID != first.Principal.AccountID || second.Principal.AccountID == admin.ID ||
		second.Principal.SessionID == first.Principal.SessionID {
		t.Fatalf("Cognito identity was not reused independently of local accounts: %v", err)
	}
	var identities, grants int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_identities").Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE account_id = ?", first.Principal.AccountID).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if identities != 1 || grants != 0 {
		t.Fatal("repeated Cognito login created extra identities or grants")
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", first.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, first.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("disabled Cognito identity retained a session: %v", err)
	}
	_, thirdExchange := cognitoSessionExchange(t, f)
	if denied, err := manager.IssueCognito(ctx, thirdExchange); !errors.Is(err, session.ErrCognitoSessionUnavailable) || denied.Token != "" {
		t.Fatalf("disabled Cognito identity received a new session: %v", err)
	}
	var sessions int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials").Scan(&sessions); err != nil || sessions != 2 {
		t.Fatalf("disabled identity changed its session count: %d %v", sessions, err)
	}
}

func TestCognitoSessionLogoutAuditFailureRollsBackRevocation(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	ctx := context.Background()
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_logout BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.logout'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := manager.Revoke(ctx, credentials.Token); err == nil {
		t.Fatal("logout audit failure was hidden")
	}
	if _, err := manager.Resolve(ctx, credentials.Token); err != nil {
		t.Fatalf("failed logout left a partially revoked session: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("failed logout partially removed refresh credentials: count=%d err=%v", retained, err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_cognito_logout"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Revoke(ctx, credentials.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("audited logout did not revoke the session: %v", err)
	}
}

func TestCognitoSessionNeverFallsBackToLinkedLocalAuthentication(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	ctx := context.Background()
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `INSERT INTO local_accounts
(identity_id, username, password_hash, must_change_password, credential_version) VALUES (?, 'linked-local', ?, 0, 1)`,
		credentials.Principal.AccountID, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', 'ADMIN')", credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	principal, err := manager.Resolve(ctx, credentials.Token)
	if err != nil || principal.Source != session.SourceCognito || principal.Username != "cognito-user" {
		t.Fatalf("linked identity changed the session authentication source: %v", err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: credentials.Principal.AccountID, SessionID: credentials.Principal.SessionID}
	if _, err := list.Execute(ctx, actor, "", 10); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("Cognito session was accepted as a local administrator: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE cognito_session_credentials SET claims_fresh_until_ns = ? WHERE session_id = ?",
		f.clock().UnixNano(), credentials.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("stale Cognito session fell back to the local account: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DELETE FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("Cognito session with missing credentials fell back to the local account: %v", err)
	}
}

func TestCognitoSessionReservationIsSingleUseUnderConcurrency(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	const callers = 12
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := manager.IssueCognito(context.Background(), exchange)
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
		case errors.Is(err, session.ErrCognitoSessionUnavailable):
			denied++
		default:
			t.Fatalf("unexpected concurrent session issuance error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.tokenCalls.Load() != 1 {
		t.Fatalf("concurrent issuance was not single-use: success=%d denied=%d", success, denied)
	}
}

func TestCognitoSessionAuditFailureWithholdsAndRollsBackCredentials(t *testing.T) {
	for _, outcome := range []string{"attempted", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			_, exchange := cognitoSessionExchange(t, f)
			manager, _ := cognitoSessionTools(t, f)
			ctx := context.Background()
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_session_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.session' AND NEW.outcome = '`+outcome+`'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := manager.IssueCognito(ctx, exchange)
			if !errors.Is(err, session.ErrCognitoSessionAudit) || credentials.Token != "" || credentials.CSRFToken != "" {
				t.Fatalf("audit failure returned session credentials: %v", err)
			}
			var identities, sessions int
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_identities").Scan(&identities); err != nil {
				t.Fatal(err)
			}
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials").Scan(&sessions); err != nil {
				t.Fatal(err)
			}
			if identities != 0 || sessions != 0 {
				t.Fatal("audit failure left a Cognito identity or session")
			}
			if outcome == "succeeded" {
				if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_cognito_session_audit"); err != nil {
					t.Fatal(err)
				}
				if _, err := manager.IssueCognito(ctx, exchange); !errors.Is(err, session.ErrCognitoSessionUnavailable) {
					t.Fatalf("reserved session creation was retried after audit failure: %v", err)
				}
			}
		})
	}
}

func TestCognitoSessionUnknownCommitWithholdsCredentials(t *testing.T) {
	for _, outcome := range []string{"attempted", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			_, exchange := cognitoSessionExchange(t, f)
			manager, _ := cognitoSessionTools(t, f)
			ctx := context.Background()
			_, err := f.db.db.ExecContext(ctx, `CREATE TABLE cognito_session_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_cognito_session_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.session' AND NEW.outcome = '`+outcome+`'
BEGIN INSERT INTO cognito_session_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := manager.IssueCognito(ctx, exchange)
			if !errors.Is(err, session.ErrCognitoSessionUnknown) || credentials.Token != "" || credentials.CSRFToken != "" {
				t.Fatalf("unknown commit returned session credentials: %v", err)
			}
			if outcome == "succeeded" {
				if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER fail_cognito_session_commit"); err != nil {
					t.Fatal(err)
				}
				if _, err := manager.IssueCognito(ctx, exchange); !errors.Is(err, session.ErrCognitoSessionUnavailable) {
					t.Fatalf("unknown session outcome was retried: %v", err)
				}
			}
		})
	}
}

func TestCognitoSessionRechecksQueuedExpiry(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	proof, err := exchange.ForSession(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	details, err := proof.Details(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	err = runQueuedDatabaseOperation(t, f.db, func() {
		f.current.Store(details.ValidUntil.UnixNano())
	}, func() error {
		_, err := manager.IssueCognito(context.Background(), exchange)
		return err
	})
	if !errors.Is(err, session.ErrCognitoSessionUnavailable) || !errors.Is(err, cognito.ErrCodeExchangeExpired) {
		t.Fatalf("queued expired Cognito session was accepted: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.session", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied ||
		!events[0].OccurredAt.Equal(details.ValidUntil) {
		t.Fatalf("queued Cognito expiry was not audited: count=%d err=%v", len(events), err)
	}
}

type advanceDuringEncryption struct {
	fixture  *cognitoCodeFixture
	deadline time.Time
}

func (r advanceDuringEncryption) Read(buffer []byte) (int, error) {
	n, err := rand.Reader.Read(buffer)
	r.fixture.current.Store(r.deadline.UnixNano())
	return n, err
}

func TestCognitoSessionRechecksExpiryDuringEncryption(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	proof, err := exchange.ForSession(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	details, err := proof.Details(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	protector, err = cognito.NewRefreshTokenProtector(f.manager, advanceDuringEncryption{fixture: f, deadline: details.ValidUntil}, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	manager, err = session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, protector)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueCognito(context.Background(), exchange)
	if !errors.Is(err, session.ErrCognitoSessionUnavailable) || !errors.Is(err, cognito.ErrCodeExchangeExpired) || credentials.Token != "" {
		t.Fatalf("session expiring during encryption returned credentials: %v", err)
	}
	var identities, sessions int
	if err := f.db.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM cognito_identities").Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM cognito_session_credentials").Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if identities != 0 || sessions != 0 {
		t.Fatal("expired session left identity or credentials")
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.session", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditValidationRejected {
		t.Fatalf("expiry during encryption was not audited: count=%d err=%v", len(events), err)
	}
}

func TestCognitoSessionWithholdsCredentialsIfCommitFinishesAfterExpiry(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	proof, err := exchange.ForSession(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	details, err := proof.Details(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	clockCalls := 0
	f.db.now = func() time.Time {
		clockCalls++
		if clockCalls >= 4 {
			return details.ValidUntil
		}
		return f.clock()
	}
	credentials, err := manager.IssueCognito(context.Background(), exchange)
	if !errors.Is(err, session.ErrCognitoSessionUnavailable) || !errors.Is(err, cognito.ErrCodeExchangeExpired) ||
		credentials.Token != "" || credentials.CSRFToken != "" {
		t.Fatalf("session committed after expiry returned credentials: %v", err)
	}
	var sessions int
	if err := f.db.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM cognito_session_credentials").Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("committed but undisclosed session was not retained for investigation: count=%d err=%v", sessions, err)
	}
}

func TestCognitoSessionVersionSevenMigrationPreservesLocalState(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	provider := cognito.ProviderConfig{
		Issuer: "https://issuer.example/pool", ClientID: "test-client",
		AuthorizationURL: "https://login.example/authorize", TokenURL: "https://login.example/token",
		JWKSURL: "https://issuer.example/jwks",
	}
	attempts, err := cognito.NewAttemptManager(f.db, rand.Reader, time.Now, bytes.Repeat([]byte{7}, 32), provider, "https://console.example/callback")
	if err != nil {
		t.Fatal(err)
	}
	start, err := attempts.StartAuthorization(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TABLE console_browser_sessions; ALTER TABLE console_sessions DROP COLUMN browser_binding_hash; DROP TABLE cognito_group_bindings; DROP TABLE cognito_session_credentials; DROP TABLE cognito_identities; ALTER TABLE console_sessions DROP COLUMN auth_source; PRAGMA user_version = 7"); err != nil {
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
	if err != nil || principal.AccountID != f.admin.ID || principal.Source != session.SourceLocal {
		t.Fatalf("version-seven migration lost a local session: %v", err)
	}
	attempts, err = cognito.NewAttemptManager(db, rand.Reader, time.Now, bytes.Repeat([]byte{7}, 32), provider, "https://console.example/callback")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := attempts.ConsumeAuthorization(ctx, start.State, start.BrowserBinding); err != nil {
		t.Fatalf("version-seven migration lost a pending authorization attempt: %v", err)
	}
	var version, tables int
	if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 10 {
		t.Fatalf("version-seven migration failed: version=%d err=%v", version, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
AND name IN ('cognito_identities', 'cognito_session_credentials')`).Scan(&tables); err != nil || tables != 2 {
		t.Fatalf("Cognito session tables were not migrated: count=%d err=%v", tables, err)
	}
}

func TestCognitoSessionRejectsForgedExchangeAndSupportsAdminRevocation(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	manager, _ := cognitoSessionTools(t, f)
	forged := cognito.CodeExchangeResult{OperationID: "forged", RefreshToken: fixtureRefresh, Identity: cognito.VerifiedIdentity{
		Issuer: f.provider.Issuer, Subject: "forged-subject", Audience: []string{f.provider.ClientID},
	}}
	if issued, err := manager.IssueCognito(ctx, forged); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || issued.Token != "" {
		t.Fatalf("forged exchange issued a Cognito session: %v", err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 0 WHERE identity_id = ?", admin.ID); err != nil {
		t.Fatal(err)
	}
	admin.MustChangePassword = false
	adminCredentials, err := manager.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	_, exchange := cognitoSessionExchange(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: admin.ID, SessionID: adminCredentials.Principal.SessionID}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	page, err := list.Execute(ctx, actor, "", 10)
	if err != nil || len(page.Sessions) != 2 {
		t.Fatalf("administrator could not list both session types: count=%d err=%v", len(page.Sessions), err)
	}
	found := false
	for _, item := range page.Sessions {
		if item.ID == credentials.Principal.SessionID {
			found = item.Source == "cognito" && item.AccountID == credentials.Principal.AccountID && item.Username == "cognito-user"
		}
	}
	if !found {
		t.Fatal("administrator listing omitted the Cognito session or its authentication source")
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewSetLocalAccountEnabled(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	handler := adminSessionsTestHandler(t, accountAdministrationFixture{
		db: f.db, manager: manager, change: change, admin: admin, adminRun: adminCredentials,
	}, list, revoke, &logs)
	listResponse := adminSessionsTestRequest(handler, adminCredentials, http.MethodGet, "/api/admin/sessions")
	var listed struct {
		Sessions []struct {
			ID     string `json:"id"`
			Source string `json:"authSource"`
		} `json:"sessions"`
	}
	if listResponse.Code != http.StatusOK || json.Unmarshal(listResponse.Body.Bytes(), &listed) != nil {
		t.Fatalf("administrator HTTP listing failed: %d", listResponse.Code)
	}
	found = false
	for _, item := range listed.Sessions {
		if item.ID == credentials.Principal.SessionID && item.Source == "cognito" {
			found = true
		}
	}
	if !found {
		t.Fatal("administrator HTTP listing omitted the Cognito authentication source")
	}
	inspect := adminSessionsTestRequest(handler, credentials, http.MethodGet, "/api/session")
	if inspect.Code != http.StatusOK || !strings.Contains(inspect.Body.String(), `"authSource":"cognito"`) ||
		strings.Contains(inspect.Body.String(), "opaque|synthetic-subject") {
		t.Fatalf("Cognito session inspection exposed incorrect metadata: status=%d", inspect.Code)
	}
	revokeResponse := adminSessionsTestRequest(handler, adminCredentials, http.MethodPost,
		"/api/admin/sessions/"+credentials.Principal.SessionID+"/revoke")
	if revokeResponse.Code != http.StatusOK {
		t.Fatalf("administrator HTTP revocation failed: %d", revokeResponse.Code)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("administrator-revoked Cognito session remained usable: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("administrator revocation retained Cognito refresh-token material: count=%d err=%v", retained, err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditSucceeded || events[1].ResourceID != credentials.Principal.SessionID {
		t.Fatalf("administrator revocation was not audited: count=%d err=%v", len(events), err)
	}
	if strings.Contains(events[1].Reason, fixtureRefresh) {
		t.Fatal("administrator audit exposed a provider token")
	}
	for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken, "opaque|synthetic-subject"} {
		if strings.Contains(logs.String(), secret) || strings.Contains(listResponse.Body.String(), secret) || strings.Contains(revokeResponse.Body.String(), secret) {
			t.Fatal("session administration exposed a provider or session credential")
		}
	}
}
