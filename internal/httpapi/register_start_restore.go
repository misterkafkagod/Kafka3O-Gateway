package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

type StartRestoreInput struct {
	Topic string `path:"topic" doc:"Topic whose backup is restored into it"`
}

type StartRestoreOutput struct {
	Body backup.Restore
}

func RegisterStartRestore(api huma.API, start backup.StartRestore) {
	huma.Register(api, huma.Operation{
		OperationID:   "start-restore",
		Method:        http.MethodPost,
		Path:          "/api/v1/backups/{topic}/restore",
		Summary:       "Restore a topic's backup into the topic; restored records are appended",
		DefaultStatus: http.StatusAccepted,
		Errors:        []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *StartRestoreInput) (*StartRestoreOutput, error) {
		restore, err := start.Execute(ctx, input.Topic)
		if err != nil {
			return nil, NewBackupProblem(err)
		}
		return &StartRestoreOutput{Body: restore}, nil
	})
}
