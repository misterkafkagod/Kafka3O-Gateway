package httpapi

import (
	"strconv"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func parseTopicMessagePosition(input *ListLatestTopicMessagesInput) (*topics.TopicMessagePosition, error) {
	if !input.Partition.IsSet && !input.Offset.IsSet && !input.Timestamp.IsSet {
		return nil, nil
	}
	if !input.Partition.IsSet || input.Offset.IsSet == input.Timestamp.IsSet {
		return nil, topics.ErrInvalidTopicMessagePosition
	}
	partition, err := strconv.ParseInt(input.Partition.Value, 10, 32)
	if err != nil {
		return nil, topics.ErrInvalidTopicMessagePosition
	}
	position := &topics.TopicMessagePosition{Partition: int32(partition)}
	if input.Offset.IsSet {
		offset, err := strconv.ParseInt(input.Offset.Value, 10, 64)
		if err != nil {
			return nil, topics.ErrInvalidTopicMessagePosition
		}
		position.Offset = &offset
	}
	if input.Timestamp.IsSet {
		timestamp, err := strconv.ParseInt(input.Timestamp.Value, 10, 64)
		if err != nil {
			return nil, topics.ErrInvalidTopicMessagePosition
		}
		position.Timestamp = &timestamp
	}
	return position, nil
}
