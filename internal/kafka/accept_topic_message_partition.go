package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *topicMessageReadState) accept(partition kgo.FetchTopicPartition) error {
	if partition.Topic != s.topic {
		return fmt.Errorf("unexpected topic %q in message fetch", partition.Topic)
	}
	end, ok := s.ends[partition.Partition]
	if !ok {
		return fmt.Errorf("unexpected partition %d in message fetch", partition.Partition)
	}
	if partition.Err != nil {
		return fmt.Errorf("fetch partition %d: %w", partition.Partition, partition.Err)
	}
	if s.done[partition.Partition] {
		return nil
	}
	for _, record := range partition.Records {
		if record.Offset < s.next[partition.Partition] {
			continue
		}
		if record.Offset >= end {
			s.done[partition.Partition] = true
			break
		}
		s.next[partition.Partition] = record.Offset + 1
		if !record.Attrs.IsControl() {
			s.messages = append(s.messages, topicMessageFromRecord(record))
			s.counts[partition.Partition]++
		}
		if s.counts[partition.Partition] >= s.maxMessages || s.next[partition.Partition] >= end {
			s.done[partition.Partition] = true
			break
		}
	}
	return nil
}
