package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ListLatestTopicMessagesInput struct {
	Topic     string                 `path:"topic" doc:"Topic name"`
	Partition optionalQueryParameter `query:"partition" doc:"Non-negative partition ID"`
	Offset    optionalQueryParameter `query:"offset" doc:"Non-negative starting record offset"`
	Timestamp optionalQueryParameter `query:"timestamp" doc:"Non-negative Unix-millisecond timestamp"`
}

type ListLatestTopicMessagesOutput struct {
	Body []topics.TopicMessage
}

func RegisterListLatestTopicMessages(
	api huma.API,
	listLatestTopicMessages topics.ListLatestTopicMessages,
	readTopicMessages topics.ReadTopicMessages,
) {
	huma.Register(api, huma.Operation{
		OperationID: "list-latest-topic-messages",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}/messages",
		Summary:     "Latest messages in a topic",
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ListLatestTopicMessagesInput) (*ListLatestTopicMessagesOutput, error) {
		position, err := parseTopicMessagePosition(input)
		if err != nil {
			return nil, NewInvalidPosition()
		}
		var messages []topics.TopicMessage
		if position == nil {
			messages, err = listLatestTopicMessages.Execute(ctx, input.Topic)
		} else {
			messages, err = readTopicMessages.Execute(ctx, input.Topic, *position)
		}
		if errors.Is(err, topics.ErrInvalidTopicMessagePosition) {
			return nil, NewInvalidPosition()
		}
		if errors.Is(err, topics.ErrTopicNotFound) {
			return nil, NewTopicNotFound(input.Topic)
		}
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		if messages == nil {
			messages = []topics.TopicMessage{}
		}
		return &ListLatestTopicMessagesOutput{Body: messages}, nil
	})
}
