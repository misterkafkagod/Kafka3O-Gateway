package httpapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func RegisterGetBrokerConfigurations(api huma.API, get settings.GetConfigurations) {
	huma.Register(api, huma.Operation{
		OperationID: "get-broker-configurations",
		Method:      http.MethodGet,
		Path:        "/api/v1/brokers/{brokerId}/config",
		Summary:     "Broker configuration and editability",
		Errors:      []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *BrokerConfigurationInput) (*ConfigurationsOutput, error) {
		resource := settings.Resource{Kind: settings.Broker, Name: strconv.FormatInt(int64(input.BrokerID), 10)}
		configurations, err := get.Execute(ctx, resource)
		if err != nil {
			return nil, NewConfigurationProblem(err)
		}
		return &ConfigurationsOutput{Body: configurations}, nil
	})
}
