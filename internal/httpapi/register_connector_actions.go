package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type ConnectorActionInput struct {
	Connector string `path:"connector" doc:"Connector name"`
}

type ConnectorTaskActionInput struct {
	Connector string `path:"connector" doc:"Connector name"`
	Task      int32  `path:"task" minimum:"0" doc:"Task ID"`
}

type connectorActionBody struct {
	Connector string `json:"connector"`
	Action    string `json:"action"`
	Task      *int32 `json:"task,omitempty"`
}

type ConnectorActionOutput struct {
	Body connectorActionBody
}

// RegisterConnectorActions mounts pause, resume, restart and task restart; each returns 202 because Connect
// applies actions asynchronously.
func RegisterConnectorActions(api huma.API, manage connect.ManageConnector) {
	for _, kind := range []string{connect.ActionPause, connect.ActionResume, connect.ActionRestart} {
		huma.Register(api, huma.Operation{
			OperationID:   kind + "-connector",
			Method:        http.MethodPost,
			Path:          "/api/v1/connect/connectors/{connector}/" + kind,
			Summary:       "Request a connector " + kind,
			DefaultStatus: http.StatusAccepted,
			Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
		}, func(ctx context.Context, input *ConnectorActionInput) (*ConnectorActionOutput, error) {
			if err := manage.Execute(ctx, connect.Action{Kind: kind, Connector: input.Connector}); err != nil {
				return nil, NewConnectProblem(err)
			}
			return &ConnectorActionOutput{Body: connectorActionBody{Connector: input.Connector, Action: kind}}, nil
		})
	}
	huma.Register(api, huma.Operation{
		OperationID:   "restart-connector-task",
		Method:        http.MethodPost,
		Path:          "/api/v1/connect/connectors/{connector}/tasks/{task}/restart",
		Summary:       "Request a connector task restart",
		DefaultStatus: http.StatusAccepted,
		Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ConnectorTaskActionInput) (*ConnectorActionOutput, error) {
		if err := manage.Execute(ctx, connect.Action{Kind: connect.ActionRestartTask, Connector: input.Connector, Task: input.Task}); err != nil {
			return nil, NewConnectProblem(err)
		}
		task := input.Task
		return &ConnectorActionOutput{Body: connectorActionBody{Connector: input.Connector, Action: connect.ActionRestartTask, Task: &task}}, nil
	})
}
