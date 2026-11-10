package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type fakeOffsetResetAdmin struct {
	classic   kadm.ListedGroups
	modern    kadm.ListedGroups
	described kadm.DescribedGroups
	consumers kadm.DescribedConsumerGroups
	committed kadm.OffsetResponses
	starts    kadm.ListedOffsets
	ends      kadm.ListedOffsets
	afterMs   kadm.ListedOffsets
	askedMs   int64
}

func (f *fakeOffsetResetAdmin) ListGroupsByType(_ context.Context, types []string, _ ...string) (kadm.ListedGroups, error) {
	if types[0] == "classic" {
		return f.classic, nil
	}
	return f.modern, nil
}
func (f *fakeOffsetResetAdmin) DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error) {
	return f.described, nil
}
func (f *fakeOffsetResetAdmin) DescribeConsumerGroups(context.Context, ...string) (kadm.DescribedConsumerGroups, error) {
	return f.consumers, nil
}
func (f *fakeOffsetResetAdmin) FetchOffsets(context.Context, string) (kadm.OffsetResponses, error) {
	return f.committed, nil
}
func (f *fakeOffsetResetAdmin) ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return f.starts, nil
}
func (f *fakeOffsetResetAdmin) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return f.ends, nil
}
func (f *fakeOffsetResetAdmin) ListOffsetsAfterMilli(_ context.Context, ms int64, _ ...string) (kadm.ListedOffsets, error) {
	f.askedMs = ms
	return f.afterMs, nil
}

func listed(topic string, offsets ...int64) kadm.ListedOffsets {
	partitions := map[int32]kadm.ListedOffset{}
	for i, offset := range offsets {
		partitions[int32(i)] = kadm.ListedOffset{Topic: topic, Partition: int32(i), Offset: offset}
	}
	return kadm.ListedOffsets{topic: partitions}
}

func inactiveClassicAdmin() *fakeOffsetResetAdmin {
	return &fakeOffsetResetAdmin{
		classic:   kadm.ListedGroups{"billing": {Group: "billing", ProtocolType: "consumer", State: "Empty"}},
		modern:    kadm.ListedGroups{},
		described: kadm.DescribedGroups{"billing": {Group: "billing", State: "Empty"}},
		committed: kadm.OffsetResponses{"orders": {0: {Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 7}}}},
		starts:    listed("orders", 2, 3),
		ends:      listed("orders", 10, 20),
		afterMs:   listed("orders", 5, -1),
	}
}

func TestOffsetResetPreviewTargets(t *testing.T) {
	for _, tt := range []struct {
		target groups.OffsetResetTarget
		want   []int64
	}{
		{groups.OffsetResetTarget{Kind: "earliest"}, []int64{2, 3}},
		{groups.OffsetResetTarget{Kind: "latest"}, []int64{10, 20}},
		// Partition 1 has no record at or after the timestamp, so it targets its end offset.
		{groups.OffsetResetTarget{Kind: "timestamp", Timestamp: 1700000000000}, []int64{5, 20}},
	} {
		admin := inactiveClassicAdmin()
		preview, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(admin), admin).Execute(context.Background(), "billing", "orders", tt.target)
		if err != nil {
			t.Fatalf("%s: %v", tt.target.Kind, err)
		}
		if preview.Group != "billing" || preview.Topic != "orders" || len(preview.Partitions) != 2 {
			t.Fatalf("%s: preview = %+v", tt.target.Kind, preview)
		}
		for i, partition := range preview.Partitions {
			if partition.Partition != int32(i) || partition.TargetOffset != tt.want[i] {
				t.Fatalf("%s: partition %d = %+v, want target %d", tt.target.Kind, i, partition, tt.want[i])
			}
		}
		if current := preview.Partitions[0].CurrentOffset; current == nil || *current != 7 {
			t.Fatalf("%s: committed partition current = %v", tt.target.Kind, current)
		}
		if preview.Partitions[1].CurrentOffset != nil {
			t.Fatalf("%s: partition without a commit reported %d", tt.target.Kind, *preview.Partitions[1].CurrentOffset)
		}
		if tt.target.Kind == "timestamp" && admin.askedMs != 1700000000000 {
			t.Fatalf("timestamp asked = %d", admin.askedMs)
		}
	}
}

func TestOffsetResetPreviewRefusesActiveGroups(t *testing.T) {
	classic := inactiveClassicAdmin()
	classic.described = kadm.DescribedGroups{"billing": {Group: "billing", State: "Stable", Members: []kadm.DescribedGroupMember{{MemberID: "m-1"}}}}
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(classic), classic).Execute(context.Background(), "billing", "orders", groups.OffsetResetTarget{Kind: "latest"}); !errors.Is(err, groups.ErrGroupActive) {
		t.Fatalf("active classic group: %v", err)
	}
	modern := inactiveClassicAdmin()
	modern.classic = kadm.ListedGroups{}
	modern.modern = kadm.ListedGroups{"billing": {Group: "billing", ProtocolType: "consumer", State: "Stable"}}
	modern.consumers = kadm.DescribedConsumerGroups{"billing": {Group: "billing", Members: []kadm.ConsumerGroupMember{{MemberID: "m-1"}}}}
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(modern), modern).Execute(context.Background(), "billing", "orders", groups.OffsetResetTarget{Kind: "latest"}); !errors.Is(err, groups.ErrGroupActive) {
		t.Fatalf("active consumer-protocol group: %v", err)
	}
	modern.consumers = kadm.DescribedConsumerGroups{"billing": {Group: "billing", State: "Empty"}}
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(modern), modern).Execute(context.Background(), "billing", "orders", groups.OffsetResetTarget{Kind: "latest"}); err != nil {
		t.Fatalf("inactive consumer-protocol group: %v", err)
	}
}

func TestOffsetResetPreviewRejectsUnknownAndInvalidInput(t *testing.T) {
	admin := inactiveClassicAdmin()
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(admin), admin).Execute(context.Background(), "missing", "orders", groups.OffsetResetTarget{Kind: "latest"}); !errors.Is(err, groups.ErrGroupNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
	connect := inactiveClassicAdmin()
	connect.classic = kadm.ListedGroups{"billing": {Group: "billing", ProtocolType: "connect"}}
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(connect), connect).Execute(context.Background(), "billing", "orders", groups.OffsetResetTarget{Kind: "latest"}); !errors.Is(err, groups.ErrGroupNotFound) {
		t.Fatalf("non-consumer group: %v", err)
	}
	missingTopic := inactiveClassicAdmin()
	missingTopic.ends = kadm.ListedOffsets{"nope": {-1: {Topic: "nope", Partition: -1, Err: kerr.UnknownTopicOrPartition}}}
	if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(missingTopic), missingTopic).Execute(context.Background(), "billing", "nope", groups.OffsetResetTarget{Kind: "latest"}); !errors.Is(err, groups.ErrTopicNotFound) {
		t.Fatalf("unknown topic: %v", err)
	}
	for _, target := range []groups.OffsetResetTarget{{Kind: ""}, {Kind: "middle"}, {Kind: "timestamp", Timestamp: -1}} {
		if _, err := NewComputeOffsetResetPreview(NewEnsureGroupInactive(admin), admin).Execute(context.Background(), "billing", "orders", target); !errors.Is(err, groups.ErrInvalidResetTarget) {
			t.Fatalf("target %+v: %v", target, err)
		}
	}
}
