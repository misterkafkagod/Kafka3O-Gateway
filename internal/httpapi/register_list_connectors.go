package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type ListConnectorsOutput struct {
	Body []connect.Connector
}

func RegisterListConnectors(api huma.API, list connect.ListConnectors) {
	huma.Register(api, huma.Operation{
		OperationID: "list-connectors",
		Method:      http.MethodGet,
		Path:        "/api/v1/connect/connectors",
		Summary:     "Kafka Connect connectors and task states, without configuration",
		Errors:      []int{http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, _ *struct{}) (*ListConnectorsOutput, error) {
		connectors, err := list.Execute(ctx)
		if err != nil {
			return nil, NewConnectProblem(err)
		}
		return &ListConnectorsOutput{Body: connectors}, nil
	})
}
