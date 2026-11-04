package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ListLatestTopicMessagesInput struct {
	Topic string `path:"topic" doc:"Topic name"`
}

type ListLatestTopicMessagesOutput struct {
	Body []topics.TopicMessage
}

func RegisterListLatestTopicMessages(api huma.API, listLatestTopicMessages topics.ListLatestTopicMessages) {
	huma.Register(api, huma.Operation{
		OperationID: "list-latest-topic-messages",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}/messages",
		Summary:     "Latest messages in a topic",
		Errors:      []int{http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ListLatestTopicMessagesInput) (*ListLatestTopicMessagesOutput, error) {
		messages, err := listLatestTopicMessages.Execute(ctx, input.Topic)
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
