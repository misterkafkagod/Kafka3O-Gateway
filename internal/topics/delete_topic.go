package topics

import (
	"context"
	"time"
)

type DeleteTopic interface {
	Execute(ctx context.Context, name string) error
}

type deleteTopic struct {
	remove  DeleteKafkaTopic
	timeout time.Duration
}

func NewDeleteTopic(remove DeleteKafkaTopic, timeout time.Duration) DeleteTopic {
	return deleteTopic{remove: remove, timeout: timeout}
}

func (d deleteTopic) Execute(ctx context.Context, name string) error {
	if !IsValidTopicName(name) {
		return ErrInvalidTopic
	}
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.remove.Execute(ctx, name)
}
