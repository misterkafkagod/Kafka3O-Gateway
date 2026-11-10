package metrics

import "cmp"

// CompareTopicSizes orders topics largest first, then by name, for slices.SortFunc.
func CompareTopicSizes(a, b TopicSize) int {
	return cmp.Or(cmp.Compare(b.Bytes, a.Bytes), cmp.Compare(a.Topic, b.Topic))
}
