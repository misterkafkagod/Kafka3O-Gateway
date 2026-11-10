package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

type StartBackupInput struct {
	Body struct {
		Topic string `json:"topic" minLength:"1" maxLength:"249"`
	}
}

type StartBackupOutput struct {
	Body backup.Backup
}

func RegisterStartBackup(api huma.API, start backup.StartBackup) {
	huma.Register(api, huma.Operation{
		OperationID:   "start-backup",
		Method:        http.MethodPost,
		Path:          "/api/v1/backups",
		Summary:       "Start a continuous backup of a topic",
		DefaultStatus: http.StatusCreated,
		Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *StartBackupInput) (*StartBackupOutput, error) {
		created, err := start.Execute(ctx, input.Body.Topic)
		if err != nil {
			return nil, NewBackupProblem(err)
		}
		return &StartBackupOutput{Body: created}, nil
	})
}
