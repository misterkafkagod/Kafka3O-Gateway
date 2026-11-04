package topics

import (
	"context"
	"slices"
	"time"
)

const latestTopicMessageLimit = 50

type ListLatestTopicMessages interface {
	Execute(ctx context.Context, topic string) ([]TopicMessage, error)
}

type listLatestTopicMessages struct {
	fetch   FetchTopicMessages
	timeout time.Duration
}

func NewListLatestTopicMessages(fetch FetchTopicMessages, timeout time.Duration) ListLatestTopicMessages {
	return listLatestTopicMessages{fetch: fetch, timeout: timeout}
}

func (l listLatestTopicMessages) Execute(ctx context.Context, topic string) ([]TopicMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	messages, err := l.fetch.Execute(ctx, topic)
	if err != nil {
		return nil, err
	}
	if messages == nil {
		return []TopicMessage{}, nil
	}
	slices.SortFunc(messages, CompareTopicMessagesByRecency)
	if len(messages) > latestTopicMessageLimit {
		messages = messages[:latestTopicMessageLimit]
	}
	return messages, nil
}
