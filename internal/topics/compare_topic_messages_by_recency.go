package topics

import "cmp"

func CompareTopicMessagesByRecency(a, b TopicMessage) int {
	if order := b.Timestamp.Compare(a.Timestamp); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Partition, b.Partition); order != 0 {
		return order
	}
	return cmp.Compare(b.Offset, a.Offset)
}
