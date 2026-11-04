package groups

import "cmp"

func compareConsumerGroupPartitions(a, b ConsumerGroupPartition) int {
	if order := cmp.Compare(a.Topic, b.Topic); order != 0 {
		return order
	}
	return cmp.Compare(a.Partition, b.Partition)
}
