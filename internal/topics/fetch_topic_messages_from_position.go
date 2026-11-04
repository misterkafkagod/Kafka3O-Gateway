package topics

import "context"

type FetchTopicMessagesFromPosition interface {
	Execute(ctx context.Context, topic string, position TopicMessagePosition) ([]TopicMessage, error)
}
