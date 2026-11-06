package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func RegisterAlterTopicConfiguration(api huma.API, alter settings.AlterConfiguration) {
	huma.Register(api, huma.Operation{
		OperationID: "alter-topic-configuration",
		Method:      http.MethodPost,
		Path:        "/api/v1/topics/{topic}/config",
		Summary:     "Set or reset an editable topic configuration",
		Errors:      []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *TopicConfigurationChangeInput) (*ConfigurationChangeOutput, error) {
		err := alter.Execute(ctx, settings.Resource{Kind: settings.Topic, Name: input.Topic}, input.Body)
		if err != nil {
			return nil, NewConfigurationProblem(err)
		}
		output := &ConfigurationChangeOutput{}
		output.Body.Name = input.Body.Name
		output.Body.Status = "accepted"
		return output, nil
	})
}
