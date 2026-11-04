package groups

import "cmp"

func compareConsumerGroupMembers(a, b ConsumerGroupMember) int {
	return cmp.Compare(a.ID, b.ID)
}
