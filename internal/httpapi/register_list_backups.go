package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

type ListBackupsOutput struct {
	Body struct {
		Backups []backup.Backup `json:"backups"`
	}
}

func RegisterListBackups(api huma.API, list backup.ListBackups) {
	huma.Register(api, huma.Operation{
		OperationID: "list-backups",
		Method:      http.MethodGet,
		Path:        "/api/v1/backups",
		Summary:     "Topic backups with their state and stored data",
		Errors:      []int{http.StatusNotFound, http.StatusServiceUnavailable},
	}, func(ctx context.Context, _ *struct{}) (*ListBackupsOutput, error) {
		backups, err := list.Execute(ctx)
		if err != nil {
			return nil, NewBackupProblem(err)
		}
		output := &ListBackupsOutput{}
		output.Body.Backups = backups
		return output, nil
	})
}
