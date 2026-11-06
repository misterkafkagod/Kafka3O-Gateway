package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func newBoundCognitoRefreshDispatchFixture(t *testing.T) (*boundCognitoRefreshFixture, cognito.BoundRefreshDispatchRequest) {
	t.Helper()
	f := newBoundCognitoRefreshFixture(t)
	request := f.request(t)
	reservation, err := f.db.ReserveBoundCognitoRefresh(context.Background(), request, f.binding)
	if err != nil {
		t.Fatal(err)
	}
	return f, cognito.BoundRefreshDispatchRequest{
		TokenHash: request.TokenHash, BrowserBindingHash: f.binding, Reservation: reservation, OccurredAt: f.clock(),
	}
}

func TestBoundCognitoRefreshDispatchMatchesReservation(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	ctx := context.Background()
	if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, request); err != nil {
		t.Fatalf("current bound refresh was not ready for dispatch: %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*cognito.BoundRefreshDispatchRequest)
	}{
		{"missing marker", func(r *cognito.BoundRefreshDispatchRequest) { r.BrowserBindingHash = [32]byte{} }},
		{"wrong marker", func(r *cognito.BoundRefreshDispatchRequest) {
			r.BrowserBindingHash = sha256.Sum256([]byte("other marker"))
		}},
		{"wrong token", func(r *cognito.BoundRefreshDispatchRequest) { r.TokenHash = sha256.Sum256([]byte("other token")) }},
		{"wrong operation", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.OperationID = base64.RawURLEncoding.EncodeToString(make([]byte, 16))
		}},
		{"wrong session", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.SessionID = base64.RawURLEncoding.EncodeToString(make([]byte, 16))
		}},
		{"wrong identity", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.IdentityID = "browser-local-b" }},
		{"wrong generation", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.Generation++ }},
		{"wrong nonce", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.OriginalNonceHash = sha256.Sum256([]byte("other nonce"))
		}},
		{"wrong ciphertext", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.EncryptedRefreshToken[0] ^= 1 }},
		{"wrong issuer", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.PreviousIdentity.Issuer = "https://other.invalid"
		}},
		{"wrong subject", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.PreviousIdentity.Subject = "other-subject" }},
		{"wrong audience", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.PreviousIdentity.Audience[0] = "other-client"
		}},
		{"wrong groups", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.PreviousIdentity.Groups[0] = "other-group" }},
		{"wrong issuance", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.PreviousIdentity.IssuedAt = r.Reservation.PreviousIdentity.IssuedAt.Add(-time.Nanosecond)
		}},
		{"wrong expiry", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.PreviousIdentity.ExpiresAt = r.Reservation.PreviousIdentity.ExpiresAt.Add(time.Nanosecond)
		}},
		{"wrong freshness", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.PreviousClaimsFreshUntil = r.Reservation.PreviousClaimsFreshUntil.Add(-time.Nanosecond)
		}},
		{"expired reserved idle", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.IdleExpiresAt = r.OccurredAt }},
		{"expired reserved absolute", func(r *cognito.BoundRefreshDispatchRequest) { r.Reservation.AbsoluteExpiresAt = r.OccurredAt }},
		{"extended reserved idle", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.IdleExpiresAt = r.Reservation.IdleExpiresAt.Add(time.Second)
		}},
		{"extended reserved absolute", func(r *cognito.BoundRefreshDispatchRequest) {
			r.Reservation.AbsoluteExpiresAt = r.Reservation.AbsoluteExpiresAt.Add(time.Second)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changed := request
			changed.Reservation.EncryptedRefreshToken = slices.Clone(request.Reservation.EncryptedRefreshToken)
			changed.Reservation.PreviousIdentity.Audience = slices.Clone(request.Reservation.PreviousIdentity.Audience)
			changed.Reservation.PreviousIdentity.Groups = slices.Clone(request.Reservation.PreviousIdentity.Groups)
			tt.change(&changed)
			if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, changed); err == nil {
				t.Fatal("changed bound refresh reservation was accepted")
			}
		})
	}
	if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, request); err != nil {
		t.Fatalf("invalid checks changed the original reservation: %v", err)
	}
	if _, err := f.db.UseBoundSession(ctx, request.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
		t.Fatalf("dispatch guard made an unresolved session usable: %v", err)
	}
	serialized, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || f.refreshCalls.Load() != 0 {
		t.Fatalf("dispatch guard modified audit state or called the provider: count=%d err=%v", len(events), err)
	}
	for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
		fixtureRefresh, "opaque|synthetic-subject", f.provider.Issuer, "unmapped-group"} {
		if strings.Contains(string(serialized), secret) || strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("bound dispatch request or audit exposed sensitive material")
		}
	}
	f.reopen(t)
	if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, request); err != nil {
		t.Fatalf("durable reservation failed dispatch validation after restart: %v", err)
	}
}

func TestBoundCognitoRefreshDispatchRejectsChangedState(t *testing.T) {
	for _, mode := range []string{"removed", "replaced", "session-binding", "revoked", "disabled", "expired",
		"missing-credentials", "generation", "nonce", "ciphertext", "groups", "issuer", "deadline", "outcome", "missing-reservation"} {
		t.Run(mode, func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			want := error(cognito.ErrRefreshUnavailable)
			switch mode {
			case "removed":
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:]); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
				replacement, _ := browserSessionRecord("dispatch-replacement", "browser-local-b", f.binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
			case "session-binding":
				other := sha256.Sum256([]byte("changed session marker"))
				if _, err := f.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = ? WHERE id = ?", other[:], request.Reservation.SessionID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, request.TokenHash, f.binding, "revoke-before-bound-dispatch", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
				request.OccurredAt = f.clock()
			case "missing-credentials":
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", request.Reservation.SessionID); err != nil {
					t.Fatal(err)
				}
			case "generation", "nonce", "ciphertext", "groups", "issuer", "deadline":
				var statement string
				var args []any
				switch mode {
				case "generation":
					statement = "UPDATE cognito_session_credentials SET refresh_generation = 2 WHERE session_id = ?"
					args = []any{request.Reservation.SessionID}
				case "nonce":
					value := sha256.Sum256([]byte("changed nonce"))
					statement = "UPDATE cognito_session_credentials SET original_nonce_hash = ? WHERE session_id = ?"
					args = []any{value[:], request.Reservation.SessionID}
				case "ciphertext":
					statement = "UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?"
					args = []any{bytes.Repeat([]byte{1}, 32), request.Reservation.SessionID}
				case "groups":
					statement = `UPDATE cognito_session_credentials SET groups_json = '["changed-group"]' WHERE session_id = ?`
					args = []any{request.Reservation.SessionID}
				case "issuer":
					statement = "UPDATE cognito_identities SET issuer = 'https://other.invalid' WHERE identity_id = ?"
					args = []any{request.Reservation.IdentityID}
				case "deadline":
					statement = "UPDATE console_sessions SET idle_expires_at_ns = idle_expires_at_ns - 1 WHERE id = ?"
					args = []any{request.Reservation.SessionID}
				}
				if _, err := f.db.db.Exec(statement, args...); err != nil {
					t.Fatal(err)
				}
				want = cognito.ErrRefreshState
			case "outcome":
				if err := f.db.RecordCognitoRefreshFailure(ctx, cognito.RefreshFailure{
					OperationID: request.Reservation.OperationID, SessionID: request.Reservation.SessionID,
					Generation: request.Reservation.Generation, OccurredAt: f.clock(), Status: cognito.RefreshOutcomeUnknown,
				}); err != nil {
					t.Fatal(err)
				}
			case "missing-reservation":
				if _, err := f.db.db.Exec("DELETE FROM audit_events WHERE event_id = ?",
					cognitoRefreshEventID(request.Reservation.SessionID, request.Reservation.Generation)); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, request); !errors.Is(err, want) || f.refreshCalls.Load() != 0 {
				t.Fatalf("changed bound refresh state was accepted: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshDispatchQueuedBehindReplacement(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	ctx := context.Background()
	other, _ := browserSessionRecord("queued-dispatch-other", "browser-local-b", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, other); err != nil {
		t.Fatal(err)
	}
	tx, err := f.db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE console_browser_sessions SET session_id = ? WHERE browser_hash = ?", other.SessionID, f.binding[:]); err != nil {
		t.Fatal(err)
	}
	waitCount := f.db.db.Stats().WaitCount
	done := make(chan error, 1)
	go func() { done <- f.db.ValidateBoundCognitoRefreshDispatch(ctx, request) }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for f.db.db.Stats().WaitCount == waitCount {
		select {
		case err := <-done:
			t.Fatalf("dispatch guard did not wait for replacement: %v", err)
		case <-timer.C:
			t.Fatal("dispatch guard did not reach the connection queue")
		case <-ticker.C:
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 0 {
			t.Fatalf("queued dispatch accepted a replaced binding: %v", err)
		}
	case <-timer.C:
		t.Fatal("queued dispatch guard did not finish")
	}
}

func TestBoundCognitoRefreshDispatchRejectsClockRegression(t *testing.T) {
	for _, mode := range []string{"before-session", "before-claims", "after-request", "during", "after-commit", "queued", "expiry-after-commit"} {
		t.Run(mode, func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			base := f.clock()
			switch mode {
			case "before-session":
				f.current.Store(base.Add(-time.Nanosecond).UnixNano())
				request.OccurredAt = f.clock()
			case "before-claims":
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET issued_at_ns = ? WHERE session_id = ?",
					base.Add(time.Second).UnixNano(), request.Reservation.SessionID); err != nil {
					t.Fatal(err)
				}
			case "after-request":
				f.current.Store(base.Add(-time.Nanosecond).UnixNano())
			case "during", "after-commit", "expiry-after-commit":
				calls := 0
				f.db.now = func() time.Time {
					calls++
					threshold := 2
					if mode != "during" {
						threshold = 3
					}
					if calls >= threshold {
						if mode == "expiry-after-commit" {
							return request.Reservation.IdleExpiresAt
						}
						return base.Add(-time.Nanosecond)
					}
					return base
				}
			}
			run := func() error { return f.db.ValidateBoundCognitoRefreshDispatch(ctx, request) }
			var err error
			if mode == "queued" {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(base.Add(-time.Nanosecond).UnixNano())
				}, run)
			} else {
				err = run()
			}
			want := error(ErrInvalidAuditEvent)
			if mode == "expiry-after-commit" {
				want = cognito.ErrRefreshUnavailable
			}
			if !errors.Is(err, want) || f.refreshCalls.Load() != 0 {
				t.Fatalf("dispatch guard accepted %s clock: %v", mode, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
				t.Fatal("dispatch clock failure changed the durable reservation")
			}
		})
	}
}

func TestBoundCognitoRefreshDispatchReportsCanceledCommit(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := f.clock()
	calls := 0
	f.db.now = func() time.Time {
		calls++
		if calls == 2 {
			cancel()
		}
		return base
	}
	err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, request)
	if err == nil || errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 0 {
		t.Fatalf("dispatch guard hid a canceled transaction: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
		t.Fatal("canceled dispatch validation changed the reservation")
	}
}

func TestBoundCognitoRefreshDispatchRejectsTimeBeforeReservation(t *testing.T) {
	for _, mode := range []string{"clock", "request"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			base := f.clock()
			f.current.Store(base.Add(10 * time.Second).UnixNano())
			request := f.request(t)
			reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
			if err != nil {
				t.Fatal(err)
			}
			earlier := base.Add(5 * time.Second)
			if mode == "clock" {
				f.current.Store(earlier.UnixNano())
			} else {
				f.current.Store(base.Add(12 * time.Second).UnixNano())
			}
			err = f.db.ValidateBoundCognitoRefreshDispatch(ctx, cognito.BoundRefreshDispatchRequest{
				TokenHash: request.TokenHash, BrowserBindingHash: f.binding, Reservation: reservation, OccurredAt: earlier,
			})
			if !errors.Is(err, ErrInvalidAuditEvent) || f.refreshCalls.Load() != 0 {
				t.Fatalf("dispatch accepted %s time before its reservation: %v", mode, err)
			}
		})
	}
}

func TestBoundCognitoRefreshDispatchPreservesUnboundReservationIsolation(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	ctx := context.Background()
	if err := f.db.ValidateBoundCognitoRefreshDispatch(nil, request); !errors.Is(err, cognito.ErrRefreshState) {
		t.Fatalf("dispatch guard accepted a nil context: %v", err)
	}
	missing := request
	missing.BrowserBindingHash = [32]byte{}
	if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, missing); !errors.Is(err, cognito.ErrRefreshState) {
		t.Fatalf("dispatch guard accepted an unbound reservation: %v", err)
	}
	unbound, token, protector := browserCognitoRecord(t, f.cognitoCodeFixture, "unbound-dispatch", [32]byte{})
	unbound.Session.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{82}, 16))
	principal, err := f.db.CreateCognitoSession(ctx, unbound)
	if err != nil {
		t.Fatal(err)
	}
	unboundRequest := cognitoRefreshRequest(t, session.Credentials{Principal: principal, Token: token}, protector, f.clock())
	unboundReservation, err := f.db.ReserveCognitoRefresh(ctx, unboundRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.ValidateBoundCognitoRefreshDispatch(ctx, cognito.BoundRefreshDispatchRequest{
		TokenHash: unboundRequest.TokenHash, BrowserBindingHash: f.binding, Reservation: unboundReservation, OccurredAt: f.clock(),
	}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("dispatch guard accepted a genuinely unbound reservation: %v", err)
	}
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	err = f.db.ValidateBoundCognitoRefreshDispatch(ctx, request)
	if err == nil || errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("dispatch guard hid missing browser storage: %v", err)
	}
	if !reflect.DeepEqual(request.Reservation.PreviousIdentity.Groups, []string{"unmapped-group"}) {
		t.Fatal("dispatch guard changed the reservation snapshot")
	}
}
