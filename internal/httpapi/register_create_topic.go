package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type CreateTopicInput struct {
	Body topics.CreateTopicRequest
}

type CreateTopicOutput struct {
	Body topics.CreateTopicRequest
}

func RegisterCreateTopic(api huma.API, create topics.CreateTopic) {
	huma.Register(api, huma.Operation{
		OperationID:   "create-topic",
		Method:        http.MethodPost,
		Path:          "/api/v1/topics",
		Summary:       "Create a topic",
		DefaultStatus: http.StatusCreated,
		Errors:        []int{http.StatusBadRequest, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *CreateTopicInput) (*CreateTopicOutput, error) {
		err := create.Execute(ctx, input.Body)
		if errors.Is(err, topics.ErrInvalidTopic) {
			return nil, NewInvalidTopic()
		}
		if errors.Is(err, topics.ErrTopicAlreadyExists) {
			return nil, NewTopicConflict()
		}
		if err != nil {
			return nil, NewTopicWriteUnknown("creation")
		}
		return &CreateTopicOutput{Body: input.Body}, nil
	})
}
