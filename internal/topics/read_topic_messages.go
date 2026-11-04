package topics

import (
	"context"
	"slices"
	"time"
)

const positionedTopicMessageLimit = 50

type ReadTopicMessages interface {
	Execute(ctx context.Context, topic string, position TopicMessagePosition) ([]TopicMessage, error)
}

type readTopicMessages struct {
	fetch   FetchTopicMessagesFromPosition
	timeout time.Duration
}

func NewReadTopicMessages(fetch FetchTopicMessagesFromPosition, timeout time.Duration) ReadTopicMessages {
	return readTopicMessages{fetch: fetch, timeout: timeout}
}

func (r readTopicMessages) Execute(ctx context.Context, topic string, position TopicMessagePosition) ([]TopicMessage, error) {
	if position.Partition < 0 ||
		(position.Offset == nil) == (position.Timestamp == nil) ||
		(position.Offset != nil && *position.Offset < 0) ||
		(position.Timestamp != nil && *position.Timestamp < 0) {
		return nil, ErrInvalidTopicMessagePosition
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	messages, err := r.fetch.Execute(ctx, topic, position)
	if err != nil {
		return nil, err
	}
	if messages == nil {
		return []TopicMessage{}, nil
	}
	slices.SortFunc(messages, CompareTopicMessagesByOffset)
	if len(messages) > positionedTopicMessageLimit {
		messages = messages[:positionedTopicMessageLimit]
	}
	return messages, nil
}
