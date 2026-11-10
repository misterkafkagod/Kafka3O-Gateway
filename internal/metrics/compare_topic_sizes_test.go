package metrics

import (
	"slices"
	"testing"
)

func TestCompareTopicSizes_OrdersLargestFirstThenByName(t *testing.T) {
	sizes := []TopicSize{{"b", 5}, {"a", 5}, {"c", 9}}
	slices.SortFunc(sizes, CompareTopicSizes)
	if !slices.Equal(sizes, []TopicSize{{"c", 9}, {"a", 5}, {"b", 5}}) {
		t.Fatalf("got %v", sizes)
	}
}
