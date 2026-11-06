package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestBootstrapAttemptsAreAuditedThrottledAndPersistAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "console.sqlite")
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	const username, password, wrongSecret = "admin-example", "initial passphrase", "wrong-secret"
	for i := 0; i < bootstrapAttemptLimit; i++ {
		if _, err := bootstrap.Execute(ctx, wrongSecret, username, password); !errors.Is(err, identity.ErrInvalidBootstrapSecret) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bootstrap, err = identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(ctx, secret, username, password); !errors.Is(err, identity.ErrBootstrapThrottled) {
		t.Fatalf("limit after reopening database: %v", err)
	}
	at = at.Add(bootstrapAttemptWindow)
	account, err := bootstrap.Execute(ctx, secret, username, password)
	if err != nil || account.ID == "" {
		t.Fatalf("bootstrap after window expiry: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.bootstrap", Limit: 100})
	if err != nil || len(events) != 2*bootstrapAttemptLimit+3 {
		t.Fatalf("bootstrap event count=%d err=%v", len(events), err)
	}
	for i := 0; i < 2*bootstrapAttemptLimit; i += 2 {
		attempt, result := events[i], events[i+1]
		if attempt.Outcome != AuditAttempted || result.Outcome != AuditFailed ||
			attempt.EventID != result.OperationID || result.ActorID != "anonymous" ||
			result.Source != AuditSourceAnonymous || result.Decision != "denied" {
			t.Fatalf("bootstrap rejection %d was not correlated and anonymous", i/2)
		}
	}
	denied, attempt, success := events[len(events)-3], events[len(events)-2], events[len(events)-1]
	if denied.Outcome != AuditDenied || denied.ActorID != "anonymous" || attempt.Outcome != AuditAttempted ||
		success.Outcome != AuditSucceeded || success.ActorID != account.ID || success.OperationID != attempt.EventID {
		t.Fatal("throttled or successful bootstrap audit is incorrect")
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, sensitive := range []string{secret, wrongSecret, username, password} {
		if strings.Contains(serialized, sensitive) {
			t.Fatal("bootstrap audit contains an input credential or username")
		}
	}
}

func TestConcurrentBootstrapAttemptsRespectGlobalLimit(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	const total = bootstrapAttemptLimit * 2
	results := make(chan error, total)
	var group sync.WaitGroup
	for i := 0; i < total; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := bootstrap.Execute(ctx, "wrong-secret", "admin", "initial passphrase")
			results <- err
		}()
	}
	group.Wait()
	close(results)
	var rejected, throttled int
	for err := range results {
		switch {
		case errors.Is(err, identity.ErrInvalidBootstrapSecret):
			rejected++
		case errors.Is(err, identity.ErrBootstrapThrottled):
			throttled++
		default:
			t.Errorf("unexpected attempt error: %v", err)
		}
	}
	if rejected != bootstrapAttemptLimit || throttled != bootstrapAttemptLimit {
		t.Fatalf("rejected=%d throttled=%d", rejected, throttled)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.bootstrap", Limit: 100})
	if err != nil || len(events) != 3*bootstrapAttemptLimit {
		t.Fatalf("concurrent bootstrap event count=%d err=%v", len(events), err)
	}
}

func TestBootstrapDoesNotProceedWhenAuditIsUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name      string
		condition string
	}{
		{"attempt", "NEW.outcome = 'attempted'"},
		{"failed outcome", "NEW.outcome = 'failed'"},
		{"successful outcome", "NEW.outcome = 'succeeded'"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.db.ExecContext(ctx, `CREATE TRIGGER reject_bootstrap_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.bootstrap' AND `+tt.condition+`
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
			bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			provided := secret
			if tt.name == "failed outcome" {
				provided = "wrong-secret"
			}
			account, err := bootstrap.Execute(ctx, provided, "admin", "initial passphrase")
			if err == nil || account.ID != "" {
				t.Fatal("bootstrap unexpectedly succeeded without durable audit")
			}
			if tt.name == "failed outcome" && !strings.Contains(err.Error(), "record failed bootstrap") {
				t.Fatalf("failed audit was not surfaced: %v", err)
			}
			if _, err := db.FindLocalAccount(ctx, "admin"); !errors.Is(err, identity.ErrAccountNotFound) {
				t.Fatalf("bootstrap left an account after audit failure: %v", err)
			}
		})
	}
}
