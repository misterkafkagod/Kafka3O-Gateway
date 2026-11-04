package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type SearchTopicMessagesInput struct {
	Topic string `path:"topic" doc:"Topic name"`
	Query string `query:"query" doc:"Case-sensitive search text"`
}

type SearchTopicMessagesOutput struct {
	Body topics.TopicMessageSearchResult
}

func RegisterSearchTopicMessages(api huma.API, search topics.SearchTopicMessages) {
	huma.Register(api, huma.Operation{
		OperationID: "search-topic-messages",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}/messages/search",
		Summary:     "Search a bounded recent window of topic messages",
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *SearchTopicMessagesInput) (*SearchTopicMessagesOutput, error) {
		result, err := search.Execute(ctx, input.Topic, input.Query)
		if errors.Is(err, topics.ErrInvalidTopicMessageSearch) {
			return nil, NewInvalidSearch()
		}
		if errors.Is(err, topics.ErrTopicNotFound) {
			return nil, NewTopicNotFound(input.Topic)
		}
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		if result.Messages == nil {
			result.Messages = []topics.TopicMessage{}
		}
		return &SearchTopicMessagesOutput{Body: result}, nil
	})
}
