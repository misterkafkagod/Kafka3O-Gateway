package groups

import "testing"

func TestCompareConsumerGroups(t *testing.T) {
	if compareConsumerGroups(ConsumerGroup{ID: "a"}, ConsumerGroup{ID: "b"}) >= 0 ||
		compareConsumerGroups(ConsumerGroup{ID: "a"}, ConsumerGroup{ID: "a"}) != 0 {
		t.Error("consumer groups are not ordered by ID")
	}
}
