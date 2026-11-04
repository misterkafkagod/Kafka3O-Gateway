package topics

import (
	"context"
	"time"
)

type GetTopicDetail interface {
	Execute(ctx context.Context, name string) (TopicDetail, error)
}

type getTopicDetail struct {
	describeTopic DescribeTopic
	timeout       time.Duration
}

func NewGetTopicDetail(describeTopic DescribeTopic, timeout time.Duration) GetTopicDetail {
	return getTopicDetail{describeTopic: describeTopic, timeout: timeout}
}

func (g getTopicDetail) Execute(ctx context.Context, name string) (TopicDetail, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	return g.describeTopic.Execute(ctx, name)
}
