package groups

import "cmp"

func compareConsumerGroups(a, b ConsumerGroup) int {
	return cmp.Compare(a.ID, b.ID)
}
