package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerIssueSerializesLocalAndCognito(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	account := identity.LocalAccount{ID: "browser-local-a", Enabled: true, CredentialVersion: 1}
	type issued struct {
		credentials session.Credentials
		err         error
	}
	start := make(chan struct{})
	results := make(chan issued, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		credentials, err := manager.IssueBoundLocal(ctx, account, binding)
		results <- issued{credentials: credentials, err: err}
	}()
	go func() {
		defer workers.Done()
		<-start
		credentials, err := manager.IssueBoundCognito(ctx, exchange, binding)
		results <- issued{credentials: credentials, err: err}
	}()
	close(start)
	workers.Wait()
	close(results)
	success, conflict := 0, 0
	var current session.Credentials
	for result := range results {
		switch result.err {
		case nil:
			success++
			current = result.credentials
		case session.ErrBrowserSessionConflict:
			conflict++
			if result.credentials.Token != "" || result.credentials.CSRFToken != "" || result.credentials.Principal.SessionID != "" {
				t.Fatal("denied concurrent issuance returned credentials")
			}
		default:
			t.Fatalf("concurrent bound issuance failed unexpectedly: %v", result.err)
		}
	}
	if success != 1 || conflict != 1 || current.Token == "" || current.CSRFToken == "" {
		t.Fatalf("mixed bound issuance did not serialize: successes=%d conflicts=%d", success, conflict)
	}
	var bindings, sessions int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_browser_sessions WHERE browser_hash = ?", binding[:]).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions WHERE browser_binding_hash = ?", binding[:]).Scan(&sessions); err != nil ||
		bindings != 1 || sessions != 1 {
		t.Fatalf("mixed bound issuance persisted competing sessions: bindings=%d sessions=%d err=%v", bindings, sessions, err)
	}
	if principal, err := manager.ResolveBound(ctx, current.Token, binding); err != nil || principal.SessionID != current.Principal.SessionID {
		t.Fatalf("winning bound session was unavailable: %v", err)
	}
}
