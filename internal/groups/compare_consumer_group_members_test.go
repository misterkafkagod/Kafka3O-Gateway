package groups

import "testing"

func TestCompareConsumerGroupMembers(t *testing.T) {
	if compareConsumerGroupMembers(ConsumerGroupMember{ID: "a"}, ConsumerGroupMember{ID: "b"}) >= 0 ||
		compareConsumerGroupMembers(ConsumerGroupMember{ID: "a"}, ConsumerGroupMember{ID: "a"}) != 0 {
		t.Error("group members are not ordered by ID")
	}
}
