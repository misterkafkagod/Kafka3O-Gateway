package metrics

import "cmp"

// CompareTopicEndOffsets orders end-offset sums by topic name, for slices.SortFunc.
func CompareTopicEndOffsets(a, b TopicEndOffset) int {
	return cmp.Compare(a.Topic, b.Topic)
}
