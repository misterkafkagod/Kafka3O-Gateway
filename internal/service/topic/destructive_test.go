package topic_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

func TestTopicService_AlterConfig_PlanChangesFromTo(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-cfg", 1)
	f.SeedTopicConfigs("t-cfg", kafka.ConfigEntry{Name: "retention.ms", Value: "3600000", Source: kafka.SourceDefault})
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanAlterConfig(context.Background(), "t-cfg", map[string]string{"retention.ms": "60000"}, nil)
	if err != nil {
		t.Fatalf("PlanAlterConfig() error: %v", err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].From != "3600000" || plan.Changes[0].To != "60000" {
		t.Fatalf("plan.Changes = %+v, want [{retention.ms 3600000 60000}]", plan.Changes)
	}
	if plan.ConfirmTarget() != "t-cfg" {
		t.Errorf("ConfirmTarget() = %q, want t-cfg", plan.ConfirmTarget())
	}
}

func TestTopicService_AlterConfig_ResetToDefault(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-cfg", 1)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	newValue := "60000"
	if err := f.IncrementalAlterTopicConfigs(context.Background(), "t-cfg", []kafka.ConfigChange{
		{Name: "retention.ms", Value: &newValue},
	}); err != nil {
		t.Fatalf("seed set error: %v", err)
	}

	result, err := svc.AlterConfig(context.Background(), operatorCaller(), "t-cfg", nil, []string{"retention.ms"}, "t-cfg", false)
	if err != nil {
		t.Fatalf("AlterConfig(reset) error: %v", err)
	}
	found := false
	for _, c := range result.Value.Configs {
		if c.Name == "retention.ms" {
			found = true
			if c.Source != kafka.SourceDefault {
				t.Errorf("retention.ms.Source = %v, want default", c.Source)
			}
		}
	}
	if !found {
		t.Fatal("retention.ms missing from result.Value.Configs")
	}
}

func TestTopicService_AddPartitions_ToNotGreaterIsPartitionMismatch(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-parts", 2)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	_, err := svc.PlanAddPartitions(context.Background(), "t-parts", 2)
	if !core.IsCode(err, core.PartitionMismatch) {
		t.Fatalf("PlanAddPartitions(to=from) error = %v, want *core.PolicyError{Code: PartitionMismatch}", err)
	}

	_, err = svc.PlanAddPartitions(context.Background(), "t-parts", 1)
	if !core.IsCode(err, core.PartitionMismatch) {
		t.Fatalf("PlanAddPartitions(to<from) error = %v, want *core.PolicyError{Code: PartitionMismatch}", err)
	}
}

func TestTopicService_AddPartitions_PlanCarriesWarning(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-parts", 2)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanAddPartitions(context.Background(), "t-parts", 4)
	if err != nil {
		t.Fatalf("PlanAddPartitions() error: %v", err)
	}
	if plan.From != 2 || plan.To != 4 || plan.Warning == "" {
		t.Fatalf("plan = %+v, want From 2, To 4, a non-empty Warning", plan)
	}
	if plan.ConfirmTarget() != "t-parts" {
		t.Errorf("ConfirmTarget() = %q, want t-parts", plan.ConfirmTarget())
	}
}

func TestTopicService_AddPartitions_ExecutesAndAudits(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-parts", 2)
	auditor, rec := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	result, err := svc.AddPartitions(context.Background(), operatorCaller(), "t-parts", 4, "t-parts", false)
	if err != nil {
		t.Fatalf("AddPartitions() error: %v", err)
	}
	if result.DryRun || result.Value.PartitionCount != 4 {
		t.Fatalf("AddPartitions() = %+v, want DryRun false, PartitionCount 4", result)
	}

	events := rec.Events()
	if len(events) != 2 {
		t.Fatalf("Events() = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[1].Severity != audit.SeverityInfo {
		t.Errorf("RESULT.Severity = %s, want INFO (T10 is not a HIGH command)", events[1].Severity)
	}
}
