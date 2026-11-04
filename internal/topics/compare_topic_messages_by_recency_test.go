package topics

import (
	"testing"
	"time"
)

func TestCompareTopicMessagesByRecency_SortsNewestThenPartitionAndOffset(t *testing.T) {
	newer := TopicMessage{Partition: 1, Offset: 1, Timestamp: time.UnixMilli(200)}
	partitionZero := TopicMessage{Partition: 0, Offset: 5, Timestamp: time.UnixMilli(100)}
	partitionZeroEarlierOffset := TopicMessage{Partition: 0, Offset: 4, Timestamp: time.UnixMilli(100)}
	partitionOne := TopicMessage{Partition: 1, Offset: 2, Timestamp: time.UnixMilli(100)}

	if got := CompareTopicMessagesByRecency(newer, partitionZero); got >= 0 {
		t.Errorf("newer comparison = %d, want < 0", got)
	}
	if got := CompareTopicMessagesByRecency(partitionZero, partitionZeroEarlierOffset); got >= 0 {
		t.Errorf("offset tie comparison = %d, want < 0", got)
	}
	if got := CompareTopicMessagesByRecency(partitionZero, partitionOne); got >= 0 {
		t.Errorf("partition tie comparison = %d, want < 0", got)
	}
}
