package topics

import "context"

type SendTopicMessage interface {
	Execute(ctx context.Context, topic string, request ProduceMessageRequest) (ProduceMessageResult, error)
}
