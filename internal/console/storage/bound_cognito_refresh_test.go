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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundCognitoRefreshFixture struct {
	*cognitoCodeFixture
	binding     [32]byte
	marker      string
	record      session.CognitoRecord
	credentials session.Credentials
	protector   *cognito.RefreshTokenProtector
	freshUntil  int64
}

func newBoundCognitoRefreshFixture(t *testing.T) *boundCognitoRefreshFixture {
	t.Helper()
	fixture, binding, marker := newBrowserSessionFixture(t)
	f := &boundCognitoRefreshFixture{cognitoCodeFixture: fixture, binding: binding, marker: marker}
	record, token, protector := browserCognitoRecord(t, fixture, "bound-refresh", binding)
	record.Session.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{80}, 16))
	principal, err := f.db.CreateCognitoSession(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	f.record, f.protector = record, protector
	f.credentials = session.Credentials{Principal: principal, Token: token}
	if err := f.db.db.QueryRow("SELECT claims_fresh_until_ns FROM cognito_session_credentials WHERE session_id = ?",
		principal.SessionID).Scan(&f.freshUntil); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *boundCognitoRefreshFixture) request(t *testing.T) cognito.RefreshReservationRequest {
	t.Helper()
	return cognitoRefreshRequest(t, f.credentials, f.protector, f.clock())
}

func TestBoundCognitoRefreshInspectionAndReservation(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	request := f.request(t)
	wrong := sha256.Sum256([]byte("another browser marker"))
	for _, marker := range [][32]byte{{}, wrong} {
		state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, marker, f.clock())
		if err != session.ErrInvalidSession || state != 0 {
			t.Fatalf("inspection accepted a missing or wrong browser binding: %v", err)
		}
		reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, marker)
		if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
			t.Fatalf("reservation accepted a missing or wrong browser binding: %v", err)
		}
	}
	if state, err := f.db.InspectCognitoRefresh(ctx, request.TokenHash, f.clock()); err != session.ErrInvalidSession || state != 0 {
		t.Fatalf("unbound inspection accepted a bound session: %v", err)
	}
	if reservation, err := f.db.ReserveCognitoRefresh(ctx, f.request(t)); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
		!reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
		t.Fatalf("unbound reservation accepted a bound session: %v", err)
	}
	f.current.Store(f.freshUntil - 1)
	if state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock()); err != nil || state != session.CognitoRefreshFresh {
		t.Fatalf("bound claims became stale before their deadline: state=%d err=%v", state, err)
	}
	f.current.Store(f.freshUntil)
	if state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock()); err != nil || state != session.CognitoRefreshReady {
		t.Fatalf("bound claims remained fresh at their deadline: state=%d err=%v", state, err)
	}
	request = f.request(t)
	reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
	if err != nil || reservation.OperationID != request.OperationID || reservation.SessionID != f.credentials.Principal.SessionID ||
		reservation.IdentityID != f.credentials.Principal.AccountID || reservation.Generation != 1 {
		t.Fatalf("bound refresh did not reserve the current session: %v", err)
	}
	if token, err := f.protector.Open(cognito.RefreshTokenBinding{
		SessionID: reservation.SessionID, IdentityID: reservation.IdentityID, Generation: reservation.Generation,
	}, reservation.EncryptedRefreshToken); err != nil || token != fixtureRefresh {
		t.Fatalf("bound reservation returned invalid protected credentials: %v", err)
	}
	if state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock()); err != nil || state != session.CognitoRefreshBlocked {
		t.Fatalf("bound reservation did not block the generation: state=%d err=%v", state, err)
	}
	if _, err := f.db.UseBoundSession(ctx, request.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
		t.Fatalf("unresolved bound refresh permitted session use: %v", err)
	}
	var idle int64
	if err := f.db.db.QueryRow("SELECT idle_expires_at_ns FROM console_sessions WHERE id = ?", reservation.SessionID).Scan(&idle); err != nil ||
		idle != f.credentials.Principal.IdleExpiresAt.UnixNano() {
		t.Fatal("refresh inspection or reservation extended the session")
	}
	serialized, err := json.Marshal(reservation)
	if err != nil {
		t.Fatal(err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 3 || events[0].Outcome != AuditDenied || events[1].Outcome != AuditDenied || events[2].Outcome != AuditAttempted {
		t.Fatalf("bound refresh decisions were not audited: count=%d err=%v", len(events), err)
	}
	for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
		fixtureRefresh, "opaque|synthetic-subject", f.provider.Issuer, "unmapped-group"} {
		if strings.Contains(string(serialized), secret) || strings.Contains(fmt.Sprint(events), secret) {
			t.Fatal("bound refresh output or audit exposed sensitive material")
		}
	}
	if f.refreshCalls.Load() != 0 {
		t.Fatal("storage-only refresh reservation contacted the provider")
	}
	f.reopen(t)
	if state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock()); err != nil || state != session.CognitoRefreshBlocked {
		t.Fatalf("bound reservation was not durable across restart: state=%d err=%v", state, err)
	}
	replay, err := f.db.ReserveBoundCognitoRefresh(ctx, f.request(t), f.binding)
	if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(replay, cognito.RefreshReservation{}) {
		t.Fatalf("bound reservation was replayed after restart: %v", err)
	}
}

func TestBoundCognitoRefreshRejectsUnavailableBindings(t *testing.T) {
	for _, mode := range []string{"removed", "replaced", "revoked", "disabled", "expired", "missing-credentials"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			switch mode {
			case "removed":
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:]); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
				replacement, _ := browserSessionRecord("refresh-replacement", "browser-local-b", f.binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, f.record.Session.TokenHash, f.binding, "bound-refresh-revoke", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
			case "missing-credentials":
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			request := f.request(t)
			want := error(session.ErrInvalidSession)
			if mode == "missing-credentials" {
				want = session.ErrCognitoRefreshUnavailable
			}
			state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock())
			if err != want || state != 0 {
				t.Fatalf("unavailable binding returned refresh state: state=%d err=%v", state, err)
			}
			reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
			if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
				t.Fatalf("unavailable binding reserved refresh material: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied || f.refreshCalls.Load() != 0 {
				t.Fatalf("unavailable binding was not safely denied: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestBoundCognitoRefreshConcurrentReservations(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	const callers = 8
	requests := make([]cognito.RefreshReservationRequest, callers)
	for i := range requests {
		requests[i] = f.request(t)
	}
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for _, request := range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
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
			t.Fatalf("unexpected bound reservation error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.refreshCalls.Load() != 0 {
		t.Fatalf("bound refresh was not reserved once: success=%d denied=%d", success, denied)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 20})
	if err != nil || len(events) != callers {
		t.Fatalf("concurrent bound reservations were not audited: count=%d err=%v", len(events), err)
	}
}

func TestBoundCognitoRefreshQueuedBehindReplacement(t *testing.T) {
	for _, operation := range []string{"inspect", "reserve"} {
		t.Run(operation, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			other, _ := browserSessionRecord("queued-refresh-other", "browser-local-b", [32]byte{}, f.clock())
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
			request := f.request(t)
			waitCount := f.db.db.Stats().WaitCount
			done := make(chan error, 1)
			go func() {
				if operation == "inspect" {
					_, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, request.OccurredAt)
					done <- err
					return
				}
				_, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
				done <- err
			}()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for f.db.db.Stats().WaitCount == waitCount {
				select {
				case err := <-done:
					t.Fatalf("bound refresh did not wait for replacement: %v", err)
				case <-timer.C:
					t.Fatal("bound refresh did not reach the connection queue")
				case <-ticker.C:
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				want := error(session.ErrInvalidSession)
				if operation == "reserve" {
					want = cognito.ErrRefreshUnavailable
				}
				if !errors.Is(err, want) {
					t.Fatalf("queued refresh accepted a replaced binding: %v", err)
				}
			case <-timer.C:
				t.Fatal("queued bound refresh did not finish")
			}
			var attempts int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM audit_events WHERE action = 'console.cognito.refresh' AND outcome = 'attempted'").Scan(&attempts); err != nil ||
				attempts != 0 || f.refreshCalls.Load() != 0 {
				t.Fatal("queued replacement dispatched or reserved a provider refresh")
			}
		})
	}
}
