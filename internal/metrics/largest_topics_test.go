package metrics

import (
	"fmt"
	"testing"
)

func TestLargestTopics_SumsReplicasAcrossDirectoriesAndKeepsTen(t *testing.T) {
	first := map[string]int64{"orders": 10}
	second := map[string]int64{"orders": 10}
	for i := range 12 {
		first[fmt.Sprintf("t%02d", i)] = int64(i)
	}
	got := LargestTopics([]LogDir{{Broker: 1, TopicBytes: first}, {Broker: 2, TopicBytes: second}})
	if len(got) != 10 || got[0] != (TopicSize{"orders", 20}) || got[1] != (TopicSize{"t11", 11}) || got[9] != (TopicSize{"t03", 3}) {
		t.Fatalf("got %v", got)
	}
}

func TestLargestTopics_IsEmptyWithoutDirectories(t *testing.T) {
	if got := LargestTopics(nil); got == nil || len(got) != 0 {
		t.Fatalf("got %#v", got)
	}
}
