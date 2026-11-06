package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func RegisterGetTopicConfigurations(api huma.API, get settings.GetConfigurations) {
	huma.Register(api, huma.Operation{
		OperationID: "get-topic-configurations",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}/config",
		Summary:     "Topic configuration and editability",
		Errors:      []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *TopicConfigurationInput) (*ConfigurationsOutput, error) {
		configurations, err := get.Execute(ctx, settings.Resource{Kind: settings.Topic, Name: input.Topic})
		if err != nil {
			return nil, NewConfigurationProblem(err)
		}
		return &ConfigurationsOutput{Body: configurations}, nil
	})
}
