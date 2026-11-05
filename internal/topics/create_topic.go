package topics

import (
	"context"
	"time"
)

type CreateTopic interface {
	Execute(ctx context.Context, request CreateTopicRequest) error
}

type createTopic struct {
	create  CreateKafkaTopic
	timeout time.Duration
}

func NewCreateTopic(create CreateKafkaTopic, timeout time.Duration) CreateTopic {
	return createTopic{create: create, timeout: timeout}
}

func (c createTopic) Execute(ctx context.Context, request CreateTopicRequest) error {
	if !IsValidTopicName(request.Name) || request.Partitions < 1 || request.ReplicationFactor < 1 {
		return ErrInvalidTopic
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.create.Execute(ctx, request)
}
