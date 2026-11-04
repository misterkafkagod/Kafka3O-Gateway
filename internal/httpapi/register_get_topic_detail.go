package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type GetTopicDetailInput struct {
	Topic string `path:"topic" doc:"Topic name"`
}

type GetTopicDetailOutput struct {
	Body topics.TopicDetail
}

func RegisterGetTopicDetail(api huma.API, getTopicDetail topics.GetTopicDetail) {
	huma.Register(api, huma.Operation{
		OperationID: "get-topic-detail",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}",
		Summary:     "Topic partitions, offsets, and configuration",
		Errors:      []int{http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *GetTopicDetailInput) (*GetTopicDetailOutput, error) {
		detail, err := getTopicDetail.Execute(ctx, input.Topic)
		if errors.Is(err, topics.ErrTopicNotFound) {
			return nil, NewTopicNotFound(input.Topic)
		}
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		return &GetTopicDetailOutput{Body: detail}, nil
	})
}
