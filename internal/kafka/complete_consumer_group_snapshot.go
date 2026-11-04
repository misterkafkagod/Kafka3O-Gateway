package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

func completeConsumerGroupSnapshot(data consumerGroupData, starts, ends kadm.ListedOffsets, startErr, endErr error) groups.ConsumerGroupSnapshot {
	if data.commitError != nil {
		data.problems = append(data.problems, fmt.Sprintf("fetch committed offsets: %v", data.commitError))
	}
	if len(data.topics) > 0 && startErr != nil {
		data.problems = append(data.problems, fmt.Sprintf("list start offsets: %v", startErr))
	}
	if len(data.topics) > 0 && endErr != nil {
		data.problems = append(data.problems, fmt.Sprintf("list end offsets: %v", endErr))
	}
	data.snapshot.Topics = data.topics.Topics()
	for _, topic := range data.snapshot.Topics {
		var topicStartErr, topicEndErr error
		if failure, ok := starts.Lookup(topic, -1); ok && failure.Err != nil {
			topicStartErr = failure.Err
			data.problems = append(data.problems, fmt.Sprintf("start offsets for %s: %v", topic, failure.Err))
		}
		if failure, ok := ends.Lookup(topic, -1); ok && failure.Err != nil {
			topicEndErr = failure.Err
			data.problems = append(data.problems, fmt.Sprintf("end offsets for %s: %v", topic, failure.Err))
		}
		for partition := range starts[topic] {
			if partition >= 0 {
				data.topics.Add(topic, partition)
			}
		}
		for partition := range ends[topic] {
			if partition >= 0 {
				data.topics.Add(topic, partition)
			}
		}
		if len(data.topics[topic]) == 0 && topicStartErr == nil && topicEndErr == nil && startErr == nil && endErr == nil {
			data.problems = append(data.problems, fmt.Sprintf("no partition offsets returned for topic %s", topic))
		}
		for partition := range data.topics[topic] {
			row := groups.ConsumerPartitionSnapshot{Topic: topic, Partition: partition}
			var problems []string
			if data.commitError != nil {
				problems = append(problems, fmt.Sprintf("committed offset: %v", data.commitError))
			} else if commit, ok := data.commits.Lookup(topic, partition); ok {
				if commit.Err != nil {
					problems = append(problems, fmt.Sprintf("committed offset: %v", commit.Err))
				} else if commit.At != -1 {
					row.CommittedOffset = new(commit.At)
				}
			}
			if start, ok := starts.Lookup(topic, partition); ok && start.Err == nil {
				row.StartOffset = new(start.Offset)
			} else if ok {
				problems = append(problems, fmt.Sprintf("start offset: %v", start.Err))
			} else if topicStartErr != nil {
				problems = append(problems, fmt.Sprintf("start offset: %v", topicStartErr))
			} else if startErr != nil {
				problems = append(problems, fmt.Sprintf("start offset: %v", startErr))
			} else {
				problems = append(problems, "start offset missing")
			}
			if end, ok := ends.Lookup(topic, partition); ok && end.Err == nil {
				row.EndOffset = new(end.Offset)
			} else if ok {
				problems = append(problems, fmt.Sprintf("end offset: %v", end.Err))
			} else if topicEndErr != nil {
				problems = append(problems, fmt.Sprintf("end offset: %v", topicEndErr))
			} else if endErr != nil {
				problems = append(problems, fmt.Sprintf("end offset: %v", endErr))
			} else {
				problems = append(problems, "end offset missing")
			}
			row.Error = strings.Join(problems, "; ")
			data.snapshot.Partitions = append(data.snapshot.Partitions, row)
		}
	}
	data.snapshot.Error = strings.Join(data.problems, "; ")
	return data.snapshot
}
