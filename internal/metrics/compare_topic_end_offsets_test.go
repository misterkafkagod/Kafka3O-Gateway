package metrics

import (
	"slices"
	"testing"
)

func TestCompareTopicEndOffsets_OrdersByTopic(t *testing.T) {
	offsets := []TopicEndOffset{{Topic: "b"}, {Topic: "a"}}
	slices.SortFunc(offsets, CompareTopicEndOffsets)
	if offsets[0].Topic != "a" || offsets[1].Topic != "b" {
		t.Fatalf("got %v", offsets)
	}
}
