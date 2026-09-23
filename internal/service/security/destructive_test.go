package security_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/security"
)

func TestSecurityService_DeleteUser_ConfirmIsName(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	if _, err := svc.Create(context.Background(), operatorCaller(), security.CreateParams{
		Name: "carol", Mechanism: kafka.ScramSha256, Password: "pw",
	}); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	plan, err := svc.PlanDelete(context.Background(), "carol", "")
	if err != nil {
		t.Fatalf("PlanDelete() error: %v", err)
	}
	if plan.ConfirmTarget() != "carol" {
		t.Errorf("ConfirmTarget() = %q, want carol", plan.ConfirmTarget())
	}
	if len(plan.Mechanisms) != 1 || plan.Mechanisms[0] != kafka.ScramSha256 {
		t.Errorf("Mechanisms = %+v, want [SCRAM-SHA-256]", plan.Mechanisms)
	}
}

func TestSecurityService_DeleteUser_ExecuteRemovesCredential(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	if _, err := svc.Create(context.Background(), operatorCaller(), security.CreateParams{
		Name: "erin", Mechanism: kafka.ScramSha256, Password: "pw",
	}); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	result, err := svc.Delete(context.Background(), operatorCaller(), "erin", "", "erin", false)
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	if result.Value.Deleted != "erin" {
		t.Fatalf("Delete().Value = %+v, want Deleted erin", result.Value)
	}

	_, err = f.DescribeUserSCRAMs(context.Background(), "erin")
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("DescribeUserSCRAMs(erin) after delete error = %v, want NotFound", err)
	}
}

func TestSecurityService_AlterQuota_ConfirmIsEntityDescriptor(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	user := "alice"
	entity := kafka.QuotaEntity{{Type: "user", Name: &user}}
	plan, err := svc.PlanAlterQuota(context.Background(), entity, map[string]float64{"producer_byte_rate": 1048576}, nil)
	if err != nil {
		t.Fatalf("PlanAlterQuota() error: %v", err)
	}
	if plan.ConfirmTarget() != "user:alice" {
		t.Errorf("ConfirmTarget() = %q, want user:alice", plan.ConfirmTarget())
	}
	if len(plan.Changes) != 1 || plan.Changes[0].From != nil || plan.Changes[0].To == nil || *plan.Changes[0].To != 1048576 {
		t.Errorf("Changes = %+v, want one change with From nil, To 1048576", plan.Changes)
	}
}

func TestSecurityService_AlterQuota_SetAndRemove(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testSecurityAuditor()
	svc := security.New(f, auditor, testSecurityRunner())

	user := "dave"
	entity := kafka.QuotaEntity{{Type: "user", Name: &user}}

	result, err := svc.AlterQuota(context.Background(), operatorCaller(), entity,
		map[string]float64{"producer_byte_rate": 2097152}, nil, "user:dave", false)
	if err != nil {
		t.Fatalf("AlterQuota(set) error: %v", err)
	}
	if len(result.Value.Values) != 1 || result.Value.Values[0].Key != "producer_byte_rate" || result.Value.Values[0].Value != 2097152 {
		t.Fatalf("AlterQuota(set).Value.Values = %+v, want producer_byte_rate=2097152", result.Value.Values)
	}

	result, err = svc.AlterQuota(context.Background(), operatorCaller(), entity,
		nil, []string{"producer_byte_rate"}, "user:dave", false)
	if err != nil {
		t.Fatalf("AlterQuota(remove) error: %v", err)
	}
	if len(result.Value.Values) != 0 {
		t.Fatalf("AlterQuota(remove).Value.Values = %+v, want none", result.Value.Values)
	}
}
