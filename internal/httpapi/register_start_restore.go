package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

// RestoreSelectionInput narrows a restore to record timestamps in [from, to) and to some partitions.
type RestoreSelectionInput struct {
	From       *time.Time `json:"from,omitempty" doc:"Earliest record timestamp restored (inclusive)"`
	To         *time.Time `json:"to,omitempty" doc:"Records restored are older than this (exclusive)"`
	Partitions *[]int     `json:"partitions,omitempty" maxItems:"10000" doc:"Partitions restored; all when absent"`
}

type StartRestoreInput struct {
	Topic string                 `path:"topic" doc:"Topic whose backup is restored into it"`
	Body  *RestoreSelectionInput `required:"false"`
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
		var filter backup.RestoreFilter
		if selection := input.Body; selection != nil {
			filter.From, filter.To = selection.From, selection.To
			if selection.Partitions != nil {
				filter.Partitions = append([]int{}, *selection.Partitions...)
			}
		}
		restore, err := start.Execute(ctx, input.Topic, filter)
		if err != nil {
			return nil, NewBackupProblem(err)
		}
		return &StartRestoreOutput{Body: restore}, nil
	})
}
