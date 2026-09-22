package group_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
)

func TestGroupService_Delete_ActiveIs409(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedGroup("g1", nil)
	f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	_, err := svc.PlanDeleteGroup(context.Background(), "g1")
	if !kafka.IsKind(err, kafka.KindGroupActive) {
		t.Fatalf("PlanDeleteGroup(active group) error = %v, want *kafka.Error{Kind: KindGroupActive}", err)
	}
}

func TestGroupService_RemoveMembers_AllOrListed(t *testing.T) {
	t.Parallel()

	t.Run("listed", func(t *testing.T) {
		t.Parallel()
		f := fake.New()
		f.SeedGroup("g1", nil)
		f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})
		f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m2"})
		auditor, _ := testGroupAuditor()
		svc := group.New(f, auditor, testGroupRunner())

		result, err := svc.RemoveMembers(context.Background(), operatorCaller(), "g1", []string{"m1"}, "g1", false)
		if err != nil {
			t.Fatalf("RemoveMembers() error: %v", err)
		}
		if len(result.Value.Removed) != 1 || result.Value.Removed[0] != "m1" {
			t.Fatalf("Removed = %v, want [m1]", result.Value.Removed)
		}
	})

	t.Run("all", func(t *testing.T) {
		t.Parallel()
		f := fake.New()
		f.SeedGroup("g1", nil)
		f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})
		f.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m2"})
		auditor, _ := testGroupAuditor()
		svc := group.New(f, auditor, testGroupRunner())

		result, err := svc.RemoveMembers(context.Background(), operatorCaller(), "g1", nil, "g1", false)
		if err != nil {
			t.Fatalf("RemoveMembers() error: %v", err)
		}
		if len(result.Value.Removed) != 2 {
			t.Fatalf("Removed = %v, want both members", result.Value.Removed)
		}
	})
}

func TestGroupService_Clone_TargetActiveIs409(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedGroup("g-source", map[kafka.TopicPartition]int64{{Topic: "orders", Partition: 0}: 5})
	f.SeedGroup("g-target", nil)
	f.SeedGroupMember("g-target", kafka.GroupMember{MemberID: "m1"})
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	_, err := svc.PlanCloneOffsets(context.Background(), "g-target", "g-source", nil)
	if !kafka.IsKind(err, kafka.KindGroupActive) {
		t.Fatalf("PlanCloneOffsets(active target) error = %v, want *kafka.Error{Kind: KindGroupActive}", err)
	}
}

func TestGroupService_Clone_CopiesSourceOffsets(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedGroup("g-source", map[kafka.TopicPartition]int64{
		{Topic: "orders", Partition: 0}: 5,
		{Topic: "orders", Partition: 1}: 3,
	})
	auditor, _ := testGroupAuditor()
	svc := group.New(f, auditor, testGroupRunner())

	result, err := svc.CloneOffsets(context.Background(), operatorCaller(), "g-target", "g-source", nil, "g-target", false)
	if err != nil {
		t.Fatalf("CloneOffsets() error: %v", err)
	}
	if len(result.Value.Offsets) != 2 {
		t.Fatalf("Offsets = %+v, want 2 entries", result.Value.Offsets)
	}

	got, err := f.FetchGroupOffsets(context.Background(), "g-target")
	if err != nil {
		t.Fatalf("FetchGroupOffsets() error: %v", err)
	}
	if got[kafka.TopicPartition{Topic: "orders", Partition: 0}] != 5 || got[kafka.TopicPartition{Topic: "orders", Partition: 1}] != 3 {
		t.Errorf("FetchGroupOffsets(g-target) = %v, want the source's offsets copied", got)
	}
}
