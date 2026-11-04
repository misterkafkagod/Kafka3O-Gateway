package topics

import "context"

type FetchTopicMessages interface {
	Execute(ctx context.Context, topic string) ([]TopicMessage, error)
}
