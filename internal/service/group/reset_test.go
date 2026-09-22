package group_test

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/gates"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
)

func testGroupAuditor() (*audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	return audit.NewAuditor(rec), rec
}

func testGroupRunner() core.Runner {
	return core.Runner{Check: gates.Check}
}

func operatorCaller() core.Caller {
	return core.Caller{Tier: core.TierOperator}
}

func seedOrdersTopic(f *fake.Fake) {
	f.SeedTopic("orders", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
}

func TestGroupService_Reset_EachMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		target group.ResetTarget
		want   int64
	}{
		{"earliest", group.ResetTarget{Mode: group.ModeEarliest}, 0},
		{"latest", group.ResetTarget{Mode: group.ModeLatest}, 3},
		{"offset", group.ResetTarget{Mode: group.ModeOffset, Offset: 1}, 1},
		{"explicit map", group.ResetTarget{Offsets: map[kafka.TopicPartition]int64{{Topic: "orders", Partition: 0}: 2}}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := fake.New()
			seedOrdersTopic(f)
			auditor, _ := testGroupAuditor()
			svc := group.New(f, auditor, testGroupRunner())

			plan, err := svc.PlanReset(context.Background(), "g1", tc.target, []string{"orders"})
			if err != nil {
				t.Fatalf("PlanReset() error: %v", err)
			}
			if len(plan.Offsets) != 1 || plan.Offsets[0].After != tc.want {
				t.Fatalf("plan.Offsets = %+v, want one entry with After %d", plan.Offsets, tc.want)
			}
		})
	}
}

func TestGroupService_Reset_TimestampClampsToLatestAndEarliest(t *testing.T) {
	t.Parallel()
	f := fake.New()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.SeedTopic("orders", 1,
		kafka.Record{Partition: 0, Value: []byte("a"), Timestamp: base},
		kafka.Record{Partition: 0, Value: []byte("b"), Timestamp: base.Add(time.Second)},
	)
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	// Far in the future: beyond the latest record -> clamps to the end offset.
	future, err := svc.PlanReset(context.Background(), "g1",
		group.ResetTarget{Mode: group.ModeTimestamp, TimestampMs: base.Add(1000 * time.Hour).UnixMilli()}, []string{"orders"})
	if err != nil {
		t.Fatalf("PlanReset(future) error: %v", err)
	}
	if len(future.Offsets) != 1 || future.Offsets[0].After != 2 {
		t.Fatalf("plan.Offsets = %+v, want After 2 (clamped to end)", future.Offsets)
	}

	// Far in the past: before the earliest record -> clamps to the begin offset.
	past, err := svc.PlanReset(context.Background(), "g1",
		group.ResetTarget{Mode: group.ModeTimestamp, TimestampMs: base.Add(-1000 * time.Hour).UnixMilli()}, []string{"orders"})
	if err != nil {
		t.Fatalf("PlanReset(past) error: %v", err)
	}
	if len(past.Offsets) != 1 || past.Offsets[0].After != 0 {
		t.Fatalf("plan.Offsets = %+v, want After 0 (clamped to begin)", past.Offsets)
	}
}

func TestGroupService_Reset_ActiveGroupIs409(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedOrdersTopic(f)
	f.SeedGroup("g1", nil)
	f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	_, err := svc.PlanReset(context.Background(), "g1", group.ResetTarget{Mode: group.ModeEarliest}, []string{"orders"})
	if !kafka.IsKind(err, kafka.KindGroupActive) {
		t.Fatalf("PlanReset(active group) error = %v, want *kafka.Error{Kind: KindGroupActive}", err)
	}
}

func TestGroupService_Reset_PreSeedsAbsentGroup(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedOrdersTopic(f)
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	result, err := svc.Reset(context.Background(), operatorCaller(), "g-new",
		group.ResetTarget{Mode: group.ModeLatest}, []string{"orders"}, "g-new", false)
	if err != nil {
		t.Fatalf("Reset() error: %v", err)
	}
	if result.DryRun || len(result.Value.Offsets) != 1 || result.Value.Offsets[0].After != 3 {
		t.Fatalf("Reset() = %+v, want one committed offset at 3", result)
	}

	got, err := f.FetchGroupOffsets(context.Background(), "g-new")
	if err != nil {
		t.Fatalf("FetchGroupOffsets() error: %v", err)
	}
	if got[kafka.TopicPartition{Topic: "orders", Partition: 0}] != 3 {
		t.Errorf("FetchGroupOffsets() = %v, want {orders/0: 3}", got)
	}
}

func TestGroupService_Reset_PlanBeforeAfter(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedOrdersTopic(f)
	f.SeedGroup("g1", map[kafka.TopicPartition]int64{{Topic: "orders", Partition: 0}: 1})
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	plan, err := svc.PlanReset(context.Background(), "g1", group.ResetTarget{Mode: group.ModeLatest}, []string{"orders"})
	if err != nil {
		t.Fatalf("PlanReset() error: %v", err)
	}
	if len(plan.Offsets) != 1 || plan.Offsets[0].Before != 1 || plan.Offsets[0].After != 3 {
		t.Fatalf("plan.Offsets = %+v, want one entry {Before:1 After:3}", plan.Offsets)
	}
	if plan.ConfirmTarget() != "g1" {
		t.Errorf("ConfirmTarget() = %q, want g1", plan.ConfirmTarget())
	}
}
