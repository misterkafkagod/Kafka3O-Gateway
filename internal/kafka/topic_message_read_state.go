package kafka

import (
	"context"
	"fmt"
	"maps"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type topicMessageReadState struct {
	topic       string
	next        map[int32]int64
	ends        map[int32]int64
	done        map[int32]bool
	counts      map[int32]int
	maxMessages int
	messages    []topics.TopicMessage
}

func newTopicMessageReadState(topic string, offsets, ends map[int32]int64, maxMessages int) (*topicMessageReadState, error) {
	if maxMessages <= 0 || len(offsets) != len(ends) {
		return nil, fmt.Errorf("invalid topic message read bounds")
	}
	state := &topicMessageReadState{
		topic:       topic,
		next:        maps.Clone(offsets),
		ends:        ends,
		done:        make(map[int32]bool, len(ends)),
		counts:      make(map[int32]int, len(ends)),
		maxMessages: maxMessages,
		messages:    make([]topics.TopicMessage, 0),
	}
	for partition, offset := range offsets {
		end, ok := ends[partition]
		if !ok || offset < 0 || end < offset {
			return nil, fmt.Errorf("invalid offsets for partition %d", partition)
		}
		if offset == end {
			state.done[partition] = true
		}
	}
	return state, nil
}

type fetchedTopicPartition struct {
	partition kgo.FetchPartition
	next      int64
	hasBatch  bool
}

type fetchTopicPartition func(context.Context, int32, int64) (fetchedTopicPartition, error)
