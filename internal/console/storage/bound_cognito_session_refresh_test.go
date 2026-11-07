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
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundRefreshingSessionFixture struct {
	*boundCognitoRefreshFixture
	sessions *session.Manager
}

func newBoundRefreshingSessionFixture(t *testing.T, mode string) *boundRefreshingSessionFixture {
	t.Helper()
	f := &boundRefreshingSessionFixture{boundCognitoRefreshFixture: newBoundCognitoRefreshFixture(t)}
	var err error
	f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
	return f
}

func (f *boundRefreshingSessionFixture) reopenManager(t *testing.T) {
	t.Helper()
	f.reopen(t)
	var err error
	f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoundCognitoSessionRefreshRevalidatesAndPersists(t *testing.T) {
	for _, mode := range []string{"refresh no rotation", "refresh rotated"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundRefreshingSessionFixture(t, mode)
			ctx := context.Background()
			principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding)
			if err != nil || principal.SessionID != f.credentials.Principal.SessionID || f.refreshCalls.Load() != 0 {
				t.Fatalf("fresh bound session triggered a refresh: %v", err)
			}
			f.current.Store(f.freshUntil)
			principal, err = f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding)
			if err != nil || principal.SessionID != f.credentials.Principal.SessionID ||
				principal.AccountID != f.credentials.Principal.AccountID || principal.Source != session.SourceCognito ||
				f.refreshCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
				t.Fatalf("stale bound session was not refreshed and revalidated: %v", err)
			}
			var generation int64
			var encrypted []byte
			var groups string
			if err := f.db.db.QueryRow(`SELECT refresh_generation, encrypted_refresh_token, groups_json
FROM cognito_session_credentials WHERE session_id = ?`, principal.SessionID).Scan(&generation, &encrypted, &groups); err != nil {
				t.Fatal(err)
			}
			wantToken := fixtureRefresh
			if mode == "refresh rotated" {
				wantToken = fixtureRotated
			}
			stored, err := f.protector.Open(cognito.RefreshTokenBinding{
				SessionID: principal.SessionID, IdentityID: principal.AccountID, Generation: 2,
			}, encrypted)
			if err != nil || stored != wantToken || generation != 2 || groups != `["refreshed-group"]` ||
				bytes.Contains(encrypted, []byte(fixtureRefresh)) || bytes.Contains(encrypted, []byte(fixtureRotated)) {
				t.Fatalf("bound manager did not persist the verified credential: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded {
				t.Fatalf("bound manager refresh was not audited: count=%d err=%v", len(events), err)
			}
			output, err := json.Marshal(principal)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
				fixtureRefresh, fixtureRotated, fixtureAccess, "opaque|synthetic-subject", f.provider.Issuer, "refreshed-group"} {
				if strings.Contains(string(output), secret) || strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("bound session response or audit exposed sensitive material")
				}
			}
			f.reopenManager(t)
			if principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding); err != nil ||
				principal.SessionID != f.credentials.Principal.SessionID || f.refreshCalls.Load() != 1 {
				t.Fatalf("refreshed bound session did not survive restart: %v", err)
			}
		})
	}
}

func TestBoundCognitoSessionRefreshRejectsWrongAndUnboundSessions(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	wrong := sha256.Sum256([]byte("different browser marker"))
	f.current.Store(f.freshUntil)
	for _, marker := range [][32]byte{{}, wrong} {
		if principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, marker); err != session.ErrInvalidSession || principal.SessionID != "" {
			t.Fatalf("bound manager accepted a missing or wrong marker: %v", err)
		}
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); err != session.ErrInvalidSession || f.refreshCalls.Load() != 0 {
		t.Fatalf("unbound manager accepted or refreshed a bound session: %v", err)
	}
	local, token := browserSessionRecord("unbound-manager", "browser-local-a", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, local); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.sessions.ResolveBound(ctx, token, wrong); err != session.ErrInvalidSession || principal.SessionID != "" {
		t.Fatalf("bound manager accepted an unbound local session: %v", err)
	}
	if principal, err := f.sessions.Resolve(ctx, token); err != nil || principal.SessionID != local.SessionID {
		t.Fatalf("bound resolution changed unbound local behavior: %v", err)
	}
	if principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding); err != nil ||
		principal.SessionID != f.credentials.Principal.SessionID || f.refreshCalls.Load() != 1 {
		t.Fatalf("wrong markers consumed the correct browser's refresh: %v", err)
	}
}

func TestBoundCognitoSessionRefreshOutageRemainsBlockedAfterRestart(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	ctx := context.Background()
	f.current.Store(f.freshUntil)
	if principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		!errors.Is(err, cognito.ErrRefreshUnknown) || errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" {
		t.Fatalf("provider outage granted access or produced a false invalid session: %v", err)
	}
	f.reopenManager(t)
	if principal, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" || f.refreshCalls.Load() != 1 {
		t.Fatalf("restart replayed or cleared an uncertain bound refresh: %v", err)
	}
	localBinding := sha256.Sum256([]byte("independent local browser"))
	local, token := browserSessionRecord("local-outage", "browser-local-a", localBinding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, local); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.sessions.ResolveBound(ctx, token, localBinding); err != nil || principal.SessionID != local.SessionID {
		t.Fatalf("provider outage blocked a bound local session: %v", err)
	}
}
