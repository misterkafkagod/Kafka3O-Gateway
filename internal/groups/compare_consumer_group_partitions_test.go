package groups

import "testing"

func TestCompareConsumerGroupPartitions(t *testing.T) {
	a := ConsumerGroupPartition{Topic: "a", Partition: 2}
	b := ConsumerGroupPartition{Topic: "b", Partition: 1}
	c := ConsumerGroupPartition{Topic: "a", Partition: 3}
	if compareConsumerGroupPartitions(a, b) >= 0 || compareConsumerGroupPartitions(a, c) >= 0 || compareConsumerGroupPartitions(a, a) != 0 {
		t.Error("partitions are not ordered by topic and partition")
	}
}
