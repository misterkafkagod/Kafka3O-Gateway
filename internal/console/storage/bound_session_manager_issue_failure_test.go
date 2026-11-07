package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerIssueWithholdsCredentialsOnLocalFailure(t *testing.T) {
	for _, mode := range []string{"missing-binding-storage", "audit", "uncertain-commit"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			manager, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing-binding-storage":
				_, err = f.db.db.Exec("DROP TABLE console_browser_sessions")
			case "audit":
				_, err = f.db.db.Exec(`CREATE TRIGGER reject_bound_manager_issue BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.start' AND NEW.outcome = 'succeeded'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			case "uncertain-commit":
				_, err = f.db.db.Exec(`CREATE TABLE manager_issue_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER fail_bound_manager_issue AFTER INSERT ON audit_events
WHEN NEW.action = 'console.session.start' AND NEW.outcome = 'succeeded'
BEGIN INSERT INTO manager_issue_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
			}
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := manager.IssueBoundLocal(ctx, identity.LocalAccount{
				ID: "browser-local-a", Enabled: true, CredentialVersion: 1,
			}, binding)
			if err == nil || err == session.ErrBrowserSessionConflict || credentials.Token != "" ||
				credentials.CSRFToken != "" || credentials.Principal.SessionID != "" {
				t.Fatalf("failed bound local issuance returned credentials or a false conflict: %v", err)
			}
			if mode == "uncertain-commit" && !errors.Is(err, session.ErrBrowserSessionUnknown) {
				t.Fatalf("bound local commit failure was not uncertain: %v", err)
			}
			var count int
			if err := f.db.db.QueryRow("SELECT COUNT(*) FROM console_sessions").Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed bound local issuance retained a session: %v", err)
			}
		})
	}
}

func TestBoundSessionManagerIssuePreservesCognitoConflictAuditFailure(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	local, err := manager.IssueBoundLocal(ctx, identity.LocalAccount{ID: "browser-local-a", Enabled: true, CredentialVersion: 1}, binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`CREATE TRIGGER reject_manager_cognito_denial BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.session' AND NEW.outcome = 'denied'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	denied, err := manager.IssueBoundCognito(ctx, exchange, binding)
	if !errors.Is(err, session.ErrCognitoSessionAudit) || err == session.ErrBrowserSessionConflict ||
		denied.Token != "" || denied.CSRFToken != "" || denied.Principal.SessionID != "" {
		t.Fatalf("bound Cognito conflict audit failure was hidden: %v", err)
	}
	if principal, err := manager.ResolveBound(ctx, local.Token, binding); err != nil || principal.SessionID != local.Principal.SessionID {
		t.Fatalf("failed Cognito issuance changed the existing session: %v", err)
	}
	if f.tokenCalls.Load() != 1 {
		t.Fatal("bound issuance retried the provider")
	}
}
