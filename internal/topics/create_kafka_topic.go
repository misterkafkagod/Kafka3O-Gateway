package topics

import "context"

type CreateKafkaTopic interface {
	Execute(ctx context.Context, request CreateTopicRequest) error
}
