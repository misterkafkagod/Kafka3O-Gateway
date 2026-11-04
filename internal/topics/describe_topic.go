package topics

import (
	"context"
	"errors"
)

var ErrTopicNotFound = errors.New("topic not found")

type DescribeTopic interface {
	Execute(ctx context.Context, name string) (TopicDetail, error)
}
