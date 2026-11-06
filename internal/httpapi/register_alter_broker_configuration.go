package httpapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func RegisterAlterBrokerConfiguration(api huma.API, alter settings.AlterConfiguration) {
	huma.Register(api, huma.Operation{
		OperationID: "alter-broker-configuration",
		Method:      http.MethodPost,
		Path:        "/api/v1/brokers/{brokerId}/config",
		Summary:     "Set or reset an editable broker configuration",
		Errors:      []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *BrokerConfigurationChangeInput) (*ConfigurationChangeOutput, error) {
		resource := settings.Resource{Kind: settings.Broker, Name: strconv.FormatInt(int64(input.BrokerID), 10)}
		if err := alter.Execute(ctx, resource, input.Body); err != nil {
			return nil, NewConfigurationProblem(err)
		}
		output := &ConfigurationChangeOutput{}
		output.Body.Name = input.Body.Name
		output.Body.Status = "accepted"
		return output, nil
	})
}
