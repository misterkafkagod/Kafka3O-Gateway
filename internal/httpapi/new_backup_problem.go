package httpapi

import (
	"errors"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

// NewBackupProblem maps backup errors to problems; Connect failures use the Connect mapping.
func NewBackupProblem(err error) *Problem {
	switch {
	case errors.Is(err, backup.ErrBackupsNotConfigured):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Backups not configured",
			Detail: "Backups are not configured for this cluster"}, Code: "backups_not_configured"}
	case errors.Is(err, backup.ErrInvalidTopic):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusBadRequest, Title: "Invalid topic",
			Detail: "the topic name is invalid"}, Code: "invalid_topic"}
	case errors.Is(err, backup.ErrBackupExists):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Backup already exists",
			Detail: "This topic already has a backup"}, Code: "backup_exists"}
	default:
		return NewConnectProblem(err)
	}
}
