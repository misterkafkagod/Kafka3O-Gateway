package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *topicMessageReadState) finish(ctx context.Context, fetch fetchTopicPartition) error {
	partitions := slices.Sorted(maps.Keys(s.ends))
	for _, partition := range partitions {
		for !s.done[partition] {
			offset := s.next[partition]
			fetched, err := fetch(ctx, partition, offset)
			if err != nil {
				return fmt.Errorf("fetch partition %d: %w", partition, err)
			}
			if fetched.partition.Partition != partition {
				return fmt.Errorf("fetch returned unexpected partition %d", fetched.partition.Partition)
			}
			if fetched.partition.Err != nil {
				return fmt.Errorf("fetch partition %d: %w", partition, fetched.partition.Err)
			}
			if fetched.partition.HighWatermark < s.ends[partition] {
				return fmt.Errorf("partition %d high watermark is below the snapshot end", partition)
			}
			if fetched.hasBatch && fetched.next <= offset {
				return fmt.Errorf("fetch partition %d made no offset progress", partition)
			}
			if err := s.accept(kgo.FetchTopicPartition{Topic: s.topic, FetchPartition: fetched.partition}); err != nil {
				return err
			}
			if s.done[partition] {
				continue
			}
			if !fetched.hasBatch {
				s.done[partition] = true
				continue
			}
			s.next[partition] = max(s.next[partition], fetched.next)
			if s.next[partition] >= s.ends[partition] {
				s.done[partition] = true
			}
		}
	}
	return nil
}
