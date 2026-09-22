package topic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

func TestTopicService_Delete_MissingIsNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	_, err := svc.PlanDelete(context.Background(), "t-missing")
	var ke *kafka.Error
	if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
		t.Fatalf("PlanDelete(missing) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}

func TestTopicService_Delete_PlanApproxMessages(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-del", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 1, Value: []byte("c")},
	)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanDelete(context.Background(), "t-del")
	if err != nil {
		t.Fatalf("PlanDelete() error: %v", err)
	}
	if plan.Partitions != 2 || plan.ApproxMessages != 3 {
		t.Fatalf("plan = %+v, want Partitions 2, ApproxMessages 3", plan)
	}
	if plan.ConfirmTarget() != "t-del" {
		t.Errorf("ConfirmTarget() = %q, want t-del", plan.ConfirmTarget())
	}
}

func TestTopicService_BulkDelete_PatternResolvesSortedTopics(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("tmp-b", 1)
	f.SeedTopic("tmp-a", 1)
	f.SeedTopic("other", 1)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanBulkDelete(context.Background(), nil, "^tmp-")
	if err != nil {
		t.Fatalf("PlanBulkDelete() error: %v", err)
	}
	if len(plan.Topics) != 2 || plan.Topics[0] != "tmp-a" || plan.Topics[1] != "tmp-b" {
		t.Fatalf("plan.Topics = %v, want [tmp-a tmp-b]", plan.Topics)
	}
	if plan.ConfirmTarget() != core.Token("T8", plan.Topics) {
		t.Errorf("ConfirmTarget() = %q, want the plan token over %v", plan.ConfirmTarget(), plan.Topics)
	}
}

func TestTopicService_BulkDelete_ListMode(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanBulkDelete(context.Background(), []string{"b", "a", "a"}, "")
	if err != nil {
		t.Fatalf("PlanBulkDelete() error: %v", err)
	}
	if len(plan.Topics) != 2 || plan.Topics[0] != "a" || plan.Topics[1] != "b" {
		t.Fatalf("plan.Topics = %v, want [a b] (sorted, de-duplicated)", plan.Topics)
	}
}

func TestTopicService_BulkDelete_StaleTokenMismatchCarriesFreshPlan(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("tmp-a", 1)
	f.SeedTopic("tmp-b", 1)
	auditor, rec := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	stale, err := svc.PlanBulkDelete(context.Background(), nil, "^tmp-")
	if err != nil {
		t.Fatalf("PlanBulkDelete() error: %v", err)
	}

	f.SeedTopic("tmp-c", 1)

	_, err = svc.BulkDelete(context.Background(), operatorCaller(), nil, "^tmp-", stale.PlanToken, false)
	var pe *core.PolicyError
	if !errors.As(err, &pe) || pe.Code != core.ConfirmationMismatch {
		t.Fatalf("BulkDelete(stale token) error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	fresh, ok := pe.Details["plan"].(topic.BulkDeletePlan)
	if !ok || len(fresh.Topics) != 3 {
		t.Fatalf("Details[plan] = %v, want a fresh plan with 3 topics", pe.Details["plan"])
	}
	if len(rec.Events()) != 0 {
		t.Errorf("Events() = %+v, want none (a confirm mismatch reports nothing)", rec.Events())
	}
}

func TestTopicService_DeleteRecords_PlanPerPartition(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-recs", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	plan, err := svc.PlanDeleteRecords(context.Background(), "t-recs", map[int32]int64{0: 2})
	if err != nil {
		t.Fatalf("PlanDeleteRecords() error: %v", err)
	}
	if len(plan.Partitions) != 1 {
		t.Fatalf("plan.Partitions = %+v, want exactly one", plan.Partitions)
	}
	got := plan.Partitions[0]
	if got.Partition != 0 || got.BeginOffset != 0 || got.TruncateTo != 2 || got.ApproxRecordsAffected != 2 {
		t.Errorf("plan.Partitions[0] = %+v, want {0 0 2 2}", got)
	}
	if plan.ConfirmTarget() != "t-recs" {
		t.Errorf("ConfirmTarget() = %q, want t-recs", plan.ConfirmTarget())
	}
}

func TestTopicService_DeleteRecords_TruncateAboveEndInvalid(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-recs", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	_, err := svc.PlanDeleteRecords(context.Background(), "t-recs", map[int32]int64{0: 999})
	if !core.IsCode(err, core.Validation) {
		t.Fatalf("PlanDeleteRecords(truncateTo beyond end) error = %v, want *core.PolicyError{Code: Validation}", err)
	}
}

func TestTopicService_Purge_EqualsDeleteRecordsAtEndOffsets(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-purge", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 1, Value: []byte("c")},
	)
	auditor, _ := testTopicAuditor()
	svc := topic.New(f, auditor, testTopicRunner())

	result, err := svc.Purge(context.Background(), operatorCaller(), "t-purge", "t-purge", false)
	if err != nil {
		t.Fatalf("Purge() error: %v", err)
	}
	byID := map[int32]int64{}
	for _, p := range result.Value.Partitions {
		byID[p.ID] = p.LowWatermark
	}
	if byID[0] != 2 || byID[1] != 1 {
		t.Fatalf("Purge() partitions = %v, want {0:2 1:1} (every partition's low watermark equals its end offset)", byID)
	}

	start, err := f.ListStartOffsets(context.Background(), "t-purge")
	if err != nil {
		t.Fatalf("ListStartOffsets() error: %v", err)
	}
	if start[0] != 2 || start[1] != 1 {
		t.Errorf("ListStartOffsets() after purge = %v, want {0:2 1:1}", start)
	}
}
