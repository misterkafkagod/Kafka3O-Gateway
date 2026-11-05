package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type DeleteTopicInput struct {
	Topic string `path:"topic" doc:"Topic name"`
}

type DeleteTopicOutput struct {
	Body struct {
		Name   string `json:"name"`
		Status string `json:"status" enum:"accepted"`
	}
}

func RegisterDeleteTopic(api huma.API, remove topics.DeleteTopic) {
	huma.Register(api, huma.Operation{
		OperationID:   "delete-topic",
		Method:        http.MethodDelete,
		Path:          "/api/v1/topics/{topic}",
		Summary:       "Request asynchronous topic deletion",
		DefaultStatus: http.StatusAccepted,
		Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *DeleteTopicInput) (*DeleteTopicOutput, error) {
		err := remove.Execute(ctx, input.Topic)
		if errors.Is(err, topics.ErrInvalidTopic) {
			return nil, NewInvalidTopic()
		}
		if errors.Is(err, topics.ErrTopicNotFound) {
			return nil, NewTopicNotFound(input.Topic)
		}
		if err != nil {
			return nil, NewTopicWriteUnknown("deletion")
		}
		output := &DeleteTopicOutput{}
		output.Body.Name = input.Topic
		output.Body.Status = "accepted"
		return output, nil
	})
}
