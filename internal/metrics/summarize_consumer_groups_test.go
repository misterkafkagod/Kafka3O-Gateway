package metrics

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

func lag(value string) *string { return &value }

func TestSummarizeConsumerGroups(t *testing.T) {
	summary := SummarizeConsumerGroups([]groups.ConsumerGroup{
		{ID: "orders", State: "Stable", Members: []groups.ConsumerGroupMember{{ID: "m1"}, {ID: "m2"}}, Partitions: []groups.ConsumerGroupPartition{
			{Lag: lag("9007199254740993")}, {Lag: lag("7")}}},
		{ID: "partial", State: "Stable", Partitions: []groups.ConsumerGroupPartition{{Lag: lag("3")}, {Lag: nil}, {Lag: lag("x")}}},
		{ID: "unknown", State: "Empty", Partitions: []groups.ConsumerGroupPartition{{Lag: nil}}},
		{ID: "idle", State: "Empty"},
		{ID: "broken", State: "Dead", Error: "COORDINATOR_NOT_AVAILABLE"},
	})
	if len(summary.States) != 3 || summary.States[0] != (StateCount{State: "Dead", Count: 1}) || summary.States[1] != (StateCount{State: "Empty", Count: 2}) || summary.States[2].Count != 2 {
		t.Fatalf("states = %+v", summary.States)
	}
	byGroup := map[string]GroupLag{}
	for _, group := range summary.Groups {
		byGroup[group.Group] = group
	}
	if summary.Groups[0].Group != "orders" || *byGroup["orders"].Lag != "9007199254741000" || *byGroup["orders"].MaxPartitionLag != "9007199254740993" || byGroup["orders"].Members != 2 {
		t.Fatalf("orders = %+v", byGroup["orders"])
	}
	if *byGroup["partial"].Lag != "3" || byGroup["partial"].UnknownPartitions != 2 {
		t.Fatalf("partial = %+v", byGroup["partial"])
	}
	if byGroup["unknown"].Lag != nil || byGroup["unknown"].UnknownPartitions != 1 || byGroup["idle"].Lag != nil || byGroup["idle"].UnknownPartitions != 0 {
		t.Fatalf("unknown %+v idle %+v", byGroup["unknown"], byGroup["idle"])
	}
	if byGroup["broken"].Error != "COORDINATOR_NOT_AVAILABLE" {
		t.Fatalf("broken = %+v", byGroup["broken"])
	}
}
