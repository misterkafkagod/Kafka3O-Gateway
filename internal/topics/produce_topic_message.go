package topics

import (
	"context"
	"time"
	"unicode/utf8"
)

type ProduceTopicMessage interface {
	Execute(ctx context.Context, topic string, request ProduceMessageRequest) (ProduceMessageResult, error)
}

type produceTopicMessage struct {
	send    SendTopicMessage
	timeout time.Duration
}

func NewProduceTopicMessage(send SendTopicMessage, timeout time.Duration) ProduceTopicMessage {
	return produceTopicMessage{send: send, timeout: timeout}
}

func (p produceTopicMessage) Execute(ctx context.Context, topic string, request ProduceMessageRequest) (ProduceMessageResult, error) {
	if topic == "" || !utf8.ValidString(topic) || request.Value == nil || !utf8.ValidString(*request.Value) ||
		(request.Key != nil && !utf8.ValidString(*request.Key)) {
		return ProduceMessageResult{}, ErrInvalidProduceMessage
	}
	for _, header := range request.Headers {
		if header.Key == nil || header.Value == nil || !utf8.ValidString(*header.Key) || !utf8.ValidString(*header.Value) {
			return ProduceMessageResult{}, ErrInvalidProduceMessage
		}
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.send.Execute(ctx, topic, request)
}
