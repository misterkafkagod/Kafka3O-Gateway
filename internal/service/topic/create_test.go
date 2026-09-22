package topic_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/gates"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

func testTopicAuditor() (*audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	return audit.NewAuditor(rec), rec
}

func testTopicRunner() core.Runner {
	return core.Runner{Check: gates.Check}
}

func operatorCaller() core.Caller {
	return core.Caller{Tier: core.TierOperator}
}

func TestTopicService_Create_ValidateOnlyReturnsPlanNoMutation(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, rec := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	result, isDryRun, err := svc.Create(context.Background(), operatorCaller(), topic.CreateParams{
		Name: "t-new", Partitions: 2, ReplicationFactor: 1,
	}, true)
	if err != nil {
		t.Fatalf("Create(dryRun) error: %v", err)
	}
	if !isDryRun || result.Name != "t-new" || result.Partitions != 2 {
		t.Fatalf("Create(dryRun) = %+v (isDryRun=%v), want name t-new, 2 partitions", result, isDryRun)
	}

	if _, err := f.DescribeTopics(context.Background(), "t-new"); !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("DescribeTopics(t-new) after dryRun = %v, want NotFound (nothing created)", err)
	}
	if calls := f.MutatingCalls(); len(calls) != 0 {
		t.Errorf("MutatingCalls() = %+v, want none", calls)
	}

	events := rec.Events()
	if len(events) != 1 || events[0].Phase != audit.PhaseResult || !events[0].DryRun {
		t.Fatalf("Events() = %+v, want exactly one dry-run RESULT (no ATTEMPT)", events)
	}
}

func TestTopicService_Create_ExistingIsAlreadyExists(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, rec := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	if _, _, err := svc.Create(context.Background(), operatorCaller(), topic.CreateParams{
		Name: "t-dup", Partitions: 1, ReplicationFactor: 1,
	}, false); err != nil {
		t.Fatalf("first Create() error: %v", err)
	}

	_, _, err := svc.Create(context.Background(), operatorCaller(), topic.CreateParams{
		Name: "t-dup", Partitions: 1, ReplicationFactor: 1,
	}, false)
	if !kafka.IsKind(err, kafka.KindAlreadyExists) {
		t.Fatalf("Create(existing) error = %v, want *kafka.Error{Kind: AlreadyExists}", err)
	}

	// Each Create() call gets its own ATTEMPT/RESULT pair — T5 has no
	// separate plan stage the way core.Destructive's destructive commands
	// do, so even an AlreadyExists failure is only discoverable by actually
	// attempting the create.
	events := rec.Events()
	if len(events) != 4 {
		t.Fatalf("Events() = %+v, want 4 (ATTEMPT+RESULT per call)", events)
	}
	if events[3].Outcome != audit.OutcomeFailed {
		t.Errorf("second call's RESULT.Outcome = %s, want FAILED", events[3].Outcome)
	}
}

func TestTopicService_CreateBulk_ExistingNameFailsValidationNothingCreated(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("already-there", 1)
	auditor, rec := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	_, err := svc.CreateBulk(context.Background(), operatorCaller(), []topic.CreateParams{
		{Name: "brand-new", Partitions: 1, ReplicationFactor: 1},
		{Name: "already-there", Partitions: 1, ReplicationFactor: 1},
	})
	if !core.IsCode(err, core.BulkValidationFailed) {
		t.Fatalf("CreateBulk() error = %v, want *core.PolicyError{Code: BulkValidationFailed}", err)
	}

	if _, err := f.DescribeTopics(context.Background(), "brand-new"); !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("DescribeTopics(brand-new) = %v, want NotFound (nothing created)", err)
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none (no ATTEMPT on validation failure)", events)
	}
}

func TestTopicService_CreateBulk_PartialFailureIsMixed(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.FailNext("CreateTopics", kafka.KindBroker)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	result, err := svc.CreateBulk(context.Background(), operatorCaller(), []topic.CreateParams{
		{Name: "t-a", Partitions: 1, ReplicationFactor: 1},
		{Name: "t-b", Partitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		t.Fatalf("CreateBulk() error: %v", err)
	}
	if result.Summary != (core.BulkSummary{Total: 2, Succeeded: 1, Failed: 1}) {
		t.Fatalf("Summary = %+v, want {Total:2 Succeeded:1 Failed:1}", result.Summary)
	}
}
