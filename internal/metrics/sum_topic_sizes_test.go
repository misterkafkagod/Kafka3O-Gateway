package metrics

import (
	"slices"
	"testing"
)

func TestSumTopicSizes_AddsReplicasAcrossDirectories(t *testing.T) {
	got := SumTopicSizes([]LogDir{{TopicBytes: map[string]int64{"orders": 4, "audit": 9}}, {TopicBytes: map[string]int64{"orders": 6}}})
	if !slices.Equal(got, []TopicSize{{"orders", 10}, {"audit", 9}}) {
		t.Fatalf("got %v", got)
	}
}
