package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

// BackupSettingsInput is the optional choice of backup settings; empty fields take the defaults.
type BackupSettingsInput struct {
	StartFrom      string `json:"startFrom,omitempty" maxLength:"16"`
	Compression    string `json:"compression,omitempty" maxLength:"16"`
	RecordsPerFile int    `json:"recordsPerFile,omitempty"`
}

type StartBackupInput struct {
	Body struct {
		Topic    string               `json:"topic" minLength:"1" maxLength:"249"`
		Settings *BackupSettingsInput `json:"settings,omitempty"`
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
		var options *backup.BackupOptions
		if chosen := input.Body.Settings; chosen != nil {
			options = &backup.BackupOptions{StartFrom: chosen.StartFrom, Compression: chosen.Compression, RecordsPerFile: chosen.RecordsPerFile}
		}
		created, err := start.Execute(ctx, input.Body.Topic, options)
		if err != nil {
			return nil, NewBackupProblem(err)
		}
		return &StartBackupOutput{Body: created}, nil
	})
}
