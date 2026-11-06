package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundCognitoRefreshAuditAndCommitFailures(t *testing.T) {
	for _, tt := range []struct {
		outcome string
		commit  bool
	}{
		{"attempted", false},
		{"denied", false},
		{"attempted", true},
		{"denied", true},
	} {
		t.Run(fmt.Sprintf("%s-commit=%t", tt.outcome, tt.commit), func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			binding := f.binding
			if tt.outcome == "denied" {
				binding = sha256.Sum256([]byte("another browser marker"))
			}
			trigger := `CREATE TRIGGER reject_bound_refresh BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
			want := cognito.ErrRefreshAudit
			if tt.commit {
				trigger = `CREATE TABLE bound_refresh_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER reject_bound_refresh AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN INSERT INTO bound_refresh_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, f.request(t), binding)
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) ||
				!reflect.DeepEqual(reservation, cognito.RefreshReservation{}) || f.refreshCalls.Load() != 0 {
				t.Fatalf("bound reservation hid an audit or commit failure: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshBadProtectorDoesNotReserve(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	wrongProtector, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
	if err != nil {
		t.Fatal(err)
	}
	request := f.request(t)
	request.Protector = wrongProtector
	reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
	if err == nil || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
		t.Fatalf("bound refresh accepted ciphertext under the wrong key: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied {
		t.Fatal("invalid protected credentials were not audited")
	}
	if _, err := f.db.ReserveBoundCognitoRefresh(ctx, f.request(t), f.binding); err != nil {
		t.Fatalf("invalid protector consumed the refresh generation: %v", err)
	}
}

func TestBoundCognitoRefreshRejectsClockRegression(t *testing.T) {
	for _, operation := range []string{"inspect", "reserve"} {
		for _, mode := range []string{"before-session", "before-claims", "after-request", "during", "after-commit", "queued", "expiry-after-commit"} {
			t.Run(operation+"-"+mode, func(t *testing.T) {
				f := newBoundCognitoRefreshFixture(t)
				ctx := context.Background()
				request := f.request(t)
				base := f.clock()
				switch mode {
				case "before-session":
					f.current.Store(base.Add(-time.Nanosecond).UnixNano())
					request.OccurredAt = f.clock()
				case "before-claims":
					if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET issued_at_ns = ? WHERE session_id = ?",
						base.Add(time.Second).UnixNano(), f.credentials.Principal.SessionID); err != nil {
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
								return f.credentials.Principal.IdleExpiresAt
							}
							return base.Add(-time.Nanosecond)
						}
						return base
					}
				}
				run := func() error {
					if operation == "inspect" {
						state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, request.OccurredAt)
						if state != 0 {
							return errors.New("invalid bound inspection returned refresh state")
						}
						return err
					}
					reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
					if !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
						return errors.New("invalid bound reservation returned material")
					}
					return err
				}
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
					want = session.ErrInvalidSession
				}
				if operation == "reserve" && (mode == "after-commit" || mode == "expiry-after-commit") {
					want = cognito.ErrRefreshUnknown
				}
				if !errors.Is(err, want) || (want == cognito.ErrRefreshUnknown && errors.Is(err, cognito.ErrRefreshUnavailable)) {
					t.Fatalf("bound %s accepted %s clock: %v", operation, mode, err)
				}
				var attempts int
				if err := f.db.db.QueryRow("SELECT COUNT(*) FROM audit_events WHERE action = 'console.cognito.refresh' AND outcome = 'attempted'").Scan(&attempts); err != nil {
					t.Fatal(err)
				}
				committed := operation == "reserve" && (mode == "after-commit" || mode == "expiry-after-commit")
				if (attempts == 1) != committed || f.refreshCalls.Load() != 0 {
					t.Fatal("bound refresh clock failure had an unexpected durable outcome")
				}
			})
		}
	}
}

func TestBoundCognitoRefreshInspectionRechecksFreshnessAfterCommit(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	request := f.request(t)
	base := f.clock()
	calls := 0
	f.db.now = func() time.Time {
		calls++
		if calls >= 3 {
			return time.Unix(0, f.freshUntil).UTC()
		}
		return base
	}
	state, err := f.db.InspectBoundCognitoRefresh(context.Background(), request.TokenHash, f.binding, request.OccurredAt)
	if err != nil || state != session.CognitoRefreshReady {
		t.Fatalf("inspection returned fresh claims after their deadline: state=%d err=%v", state, err)
	}
}

func TestBoundCognitoRefreshRejectsFutureIssuanceBeforeClockAdvances(t *testing.T) {
	for _, mode := range []string{"session", "claims"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			request := f.request(t)
			base := f.clock()
			first, later := base.Add(-time.Nanosecond), base.Add(time.Nanosecond)
			if mode == "claims" {
				first, later = base, base.Add(2*time.Second)
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET issued_at_ns = ? WHERE session_id = ?",
					base.Add(time.Second).UnixNano(), f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			request.OccurredAt = first
			calls := 0
			f.db.now = func() time.Time {
				calls++
				if calls == 1 {
					return first
				}
				return later
			}
			state, err := f.db.InspectBoundCognitoRefresh(context.Background(), request.TokenHash, f.binding, request.OccurredAt)
			if !errors.Is(err, ErrInvalidAuditEvent) || state != 0 {
				t.Fatalf("bound inspection accepted a future %s after the clock advanced: %v", mode, err)
			}
		})
	}
}

func TestBoundCognitoRefreshDenialRejectsClockRegression(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-commit=%t", afterCommit), func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			request := f.request(t)
			base := f.clock()
			calls, threshold := 0, 2
			if afterCommit {
				threshold = 3
			}
			f.db.now = func() time.Time {
				calls++
				if calls >= threshold {
					return base.Add(-time.Nanosecond)
				}
				return base
			}
			wrong := sha256.Sum256([]byte("another browser marker"))
			reservation, err := f.db.ReserveBoundCognitoRefresh(context.Background(), request, wrong)
			want := error(ErrInvalidAuditEvent)
			if afterCommit {
				want = cognito.ErrRefreshUnknown
			}
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) ||
				!reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
				t.Fatalf("bound denial hid a regressing clock: %v", err)
			}
			events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || (len(events) == 1) != afterCommit || f.refreshCalls.Load() != 0 {
				t.Fatal("bound denial clock failure had an unexpected durable outcome")
			}
		})
	}
}

func TestBoundCognitoRefreshStorageFailureAndUnboundIsolation(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	unbound, token, protector := browserCognitoRecord(t, f.cognitoCodeFixture, "unbound-refresh", [32]byte{})
	unbound.Session.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{81}, 16))
	principal, err := f.db.CreateCognitoSession(ctx, unbound)
	if err != nil {
		t.Fatal(err)
	}
	unboundCredentials := session.Credentials{Principal: principal, Token: token}
	unboundRequest := cognitoRefreshRequest(t, unboundCredentials, protector, f.clock())
	boundRequest := f.request(t)
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	state, err := f.db.InspectBoundCognitoRefresh(ctx, boundRequest.TokenHash, f.binding, f.clock())
	if err == nil || err == session.ErrInvalidSession || state != 0 {
		t.Fatalf("bound inspection hid missing browser storage: %v", err)
	}
	reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, boundRequest, f.binding)
	if err == nil || errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
		t.Fatalf("bound reservation hid missing browser storage: %v", err)
	}
	state, err = f.db.InspectCognitoRefresh(ctx, unboundRequest.TokenHash, f.clock())
	if err != nil || state != session.CognitoRefreshFresh {
		t.Fatalf("unbound inspection depended on browser storage: %v", err)
	}
	if reservation, err := f.db.ReserveCognitoRefresh(ctx, unboundRequest); err != nil || reservation.SessionID != principal.SessionID {
		t.Fatalf("unbound reservation depended on browser storage: %v", err)
	}
}
