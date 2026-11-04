package kafka

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

func TestCompleteConsumerGroupSnapshot_PreservesOffsetErrors(t *testing.T) {
	data := consumerGroupData{
		snapshot: groups.ConsumerGroupSnapshot{ID: "g"},
		topics:   kadm.TopicsSet{"orders": {0: {}, 1: {}}},
		commits: kadm.OffsetResponses{"orders": {
			0: {Offset: kadm.Offset{At: 4}},
			1: {Err: errors.New("commit denied")},
		}},
	}
	starts := kadm.ListedOffsets{"orders": {
		0: {Offset: 0}, 1: {Offset: 0},
	}}
	ends := kadm.ListedOffsets{"orders": {
		0: {Offset: 10}, 1: {Err: errors.New("end unavailable")},
	}}
	got := completeConsumerGroupSnapshot(data, starts, ends, nil, nil)
	if len(got.Partitions) != 2 {
		t.Fatalf("partitions = %#v", got.Partitions)
	}
	for _, row := range got.Partitions {
		switch row.Partition {
		case 0:
			if row.Error != "" || row.CommittedOffset == nil || *row.CommittedOffset != 4 ||
				row.StartOffset == nil || *row.StartOffset != 0 || row.EndOffset == nil || *row.EndOffset != 10 {
				t.Errorf("valid row = %#v", row)
			}
		case 1:
			if !strings.Contains(row.Error, "commit denied") || !strings.Contains(row.Error, "end unavailable") {
				t.Errorf("failed row = %#v", row)
			}
		default:
			t.Errorf("unexpected partition %d", row.Partition)
		}
	}
}

func TestCompleteConsumerGroupSnapshot_MissingCommitResponseIsUnavailable(t *testing.T) {
	data := consumerGroupData{
		snapshot: groups.ConsumerGroupSnapshot{ID: "g"},
		topics: kadm.TopicsSet{
			"orders": {0: {}},
			"gone":   {},
		},
		commitError: errors.New("not authorized"),
	}
	starts := kadm.ListedOffsets{"orders": {0: {Offset: 0}}, "gone": {-1: {Err: errors.New("unknown topic")}}}
	ends := kadm.ListedOffsets{"orders": {0: {Offset: 5}}}
	got := completeConsumerGroupSnapshot(data, starts, ends, nil, errors.New("broker unavailable"))
	if !strings.Contains(got.Error, "not authorized") || !strings.Contains(got.Error, "unknown topic") ||
		!strings.Contains(got.Error, "broker unavailable") {
		t.Errorf("group error = %q", got.Error)
	}
	if len(got.Partitions) != 1 || !strings.Contains(got.Partitions[0].Error, "not authorized") ||
		got.Partitions[0].CommittedOffset != nil {
		t.Errorf("partitions = %#v", got.Partitions)
	}
}
