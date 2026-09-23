package security_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/gates"
	"github.com/misterkafkagod/kafka3o/internal/service/security"
)

func testSecurityAuditor() (*audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	return audit.NewAuditor(rec), rec
}

func testSecurityRunner() core.Runner {
	return core.Runner{Check: gates.Check}
}

func operatorCaller() core.Caller {
	return core.Caller{Tier: core.TierOperator}
}

func TestSecurityService_CreateUser_PasswordAbsentFromAuditAndLogs(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, rec := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	const password = "s3cret-canary"
	if _, err := svc.Create(context.Background(), operatorCaller(), security.CreateParams{
		Name: "alice", Mechanism: kafka.ScramSha256, Password: password,
	}); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	events := rec.Events()
	if len(events) == 0 {
		t.Fatal("Events() = [], want at least one")
	}
	// SlogSink logs an Event by json.Marshal-ing it verbatim (audit/sink_slog.go),
	// so scanning the same marshal covers both the audit trail and "logs" —
	// there is no separate log line for a service call.
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("json.Marshal(event) error: %v", err)
		}
		if strings.Contains(string(data), password) {
			t.Errorf("audit event JSON contains the password: %s", data)
		}
	}
}

func TestSecurityService_CreateUser_IterationsDefault4096(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	if _, err := svc.Create(context.Background(), operatorCaller(), security.CreateParams{
		Name: "bob", Mechanism: kafka.ScramSha256, Password: "pw",
	}); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	users, err := f.DescribeUserSCRAMs(context.Background(), "bob")
	if err != nil {
		t.Fatalf("DescribeUserSCRAMs() error: %v", err)
	}
	if len(users[0].Credentials) != 1 || users[0].Credentials[0].Iterations != security.DefaultIterations {
		t.Fatalf("Credentials = %+v, want iterations %d", users[0].Credentials, security.DefaultIterations)
	}
}

func TestSecurityService_CreateUser_ExistingIsAlreadyExists(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	params := security.CreateParams{Name: "dup", Mechanism: kafka.ScramSha256, Password: "pw"}
	if _, err := svc.Create(context.Background(), operatorCaller(), params); err != nil {
		t.Fatalf("first Create() error: %v", err)
	}

	_, err := svc.Create(context.Background(), operatorCaller(), params)
	if !kafka.IsKind(err, kafka.KindAlreadyExists) {
		t.Fatalf("Create(existing) error = %v, want *kafka.Error{Kind: AlreadyExists}", err)
	}
}

func TestSecurityService_ListUsers_SortedByName(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	for _, name := range []string{"zeta", "alpha"} {
		if _, err := svc.Create(context.Background(), operatorCaller(), security.CreateParams{
			Name: name, Mechanism: kafka.ScramSha256, Password: "pw",
		}); err != nil {
			t.Fatalf("Create(%s) error: %v", name, err)
		}
	}

	users, err := svc.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers() error: %v", err)
	}
	if len(users) != 2 || users[0].Name != "alpha" || users[1].Name != "zeta" {
		t.Fatalf("ListUsers() = %+v, want [alpha, zeta]", users)
	}
}
