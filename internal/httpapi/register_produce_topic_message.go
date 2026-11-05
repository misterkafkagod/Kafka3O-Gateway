package httpapi

import (
	"context"
	"errors"
	"net/http"
	"unicode/utf8"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ProduceTopicMessageInput struct {
	Topic   string `path:"topic" doc:"Topic name"`
	RawBody []byte
	Body    topics.ProduceMessageRequest
}

type ProduceTopicMessageOutput struct {
	Body topics.ProduceMessageResult
}

func RegisterProduceTopicMessage(api huma.API, produce topics.ProduceTopicMessage) {
	huma.Register(api, huma.Operation{
		OperationID: "produce-topic-message",
		Method:      http.MethodPost,
		Path:        "/api/v1/topics/{topic}/messages",
		Summary:     "Produce one UTF-8 message",
		Errors:      []int{http.StatusBadRequest, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ProduceTopicMessageInput) (*ProduceTopicMessageOutput, error) {
		if !utf8.Valid(input.RawBody) {
			return nil, NewInvalidMessage()
		}
		result, err := produce.Execute(ctx, input.Topic, input.Body)
		if errors.Is(err, topics.ErrInvalidProduceMessage) {
			return nil, NewInvalidMessage()
		}
		if err != nil {
			return nil, NewDeliveryUnknown()
		}
		return &ProduceTopicMessageOutput{Body: result}, nil
	})
}
