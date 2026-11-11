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
	case errors.Is(err, backup.ErrInvalidBackupSettings):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusBadRequest, Title: "Invalid backup settings",
			Detail: err.Error()}, Code: "invalid_backup_settings"}
	case errors.Is(err, backup.ErrInvalidRestoreFilter):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusBadRequest, Title: "Invalid restore selection",
			Detail: err.Error()}, Code: "invalid_restore_filter"}
	case errors.Is(err, backup.ErrBackupNotFound):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Backup not found",
			Detail: "This topic has no backup"}, Code: "not_found"}
	case errors.Is(err, backup.ErrRestoreInProgress):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Restore in progress",
			Detail: "A restore of this topic is already running"}, Code: "restore_in_progress"}
	case errors.Is(err, backup.ErrNothingToRestore):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusNotFound, Title: "Nothing to restore",
			Detail: "This topic's backup holds no stored objects yet"}, Code: "nothing_to_restore"}
	case errors.Is(err, backup.ErrStorageUnavailable):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusServiceUnavailable, Title: "Backup storage unavailable",
			Detail: "The backup storage cannot be listed"}, Code: "storage_unavailable"}
	case errors.Is(err, backup.ErrBackupExists):
		return &Problem{ErrorModel: huma.ErrorModel{Status: http.StatusConflict, Title: "Backup already exists",
			Detail: "This topic already has a backup"}, Code: "backup_exists"}
	default:
		return NewConnectProblem(err)
	}
}
