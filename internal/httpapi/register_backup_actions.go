package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type BackupActionInput struct {
	Topic string `path:"topic" doc:"Backed-up topic"`
}

type BackupActionOutput struct {
	Body struct {
		Topic  string `json:"topic"`
		Action string `json:"action"`
	}
}

// RegisterBackupActions mounts pause, resume and delete; each answers 202 because Connect applies them asynchronously.
func RegisterBackupActions(api huma.API, manage backup.ManageBackup) {
	for _, route := range []struct {
		kind, method, suffix string
	}{
		{connect.ActionPause, http.MethodPost, "/pause"},
		{connect.ActionResume, http.MethodPost, "/resume"},
		{connect.ActionDelete, http.MethodDelete, ""},
	} {
		huma.Register(api, huma.Operation{
			OperationID:   route.kind + "-backup",
			Method:        route.method,
			Path:          "/api/v1/backups/{topic}" + route.suffix,
			Summary:       "Request a backup " + route.kind + "; stored objects are never deleted",
			DefaultStatus: http.StatusAccepted,
			Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
		}, func(ctx context.Context, input *BackupActionInput) (*BackupActionOutput, error) {
			if err := manage.Execute(ctx, input.Topic, route.kind); err != nil {
				return nil, NewBackupProblem(err)
			}
			output := &BackupActionOutput{}
			output.Body.Topic, output.Body.Action = input.Topic, route.kind
			return output, nil
		})
	}
}
