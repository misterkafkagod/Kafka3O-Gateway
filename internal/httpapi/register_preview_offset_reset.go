package httpapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type PreviewOffsetResetInput struct {
	Group     string                 `path:"group" doc:"Consumer group ID"`
	Topic     string                 `query:"topic" required:"true" minLength:"1" doc:"Topic whose offsets would be reset"`
	Target    string                 `query:"target" required:"true" enum:"earliest,latest,timestamp" doc:"Where each partition would move"`
	Timestamp optionalQueryParameter `query:"timestamp" doc:"Unix milliseconds, required for target=timestamp"`
}

type PreviewOffsetResetOutput struct {
	Body offsetResetPreviewBody
}

func RegisterPreviewOffsetReset(api huma.API, preview groups.PreviewOffsetReset) {
	huma.Register(api, huma.Operation{
		OperationID: "preview-offset-reset",
		Method:      http.MethodGet,
		Path:        "/api/v1/consumer-groups/{group}/offset-reset",
		Summary:     "Preview a consumer group offset reset without writing",
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *PreviewOffsetResetInput) (*PreviewOffsetResetOutput, error) {
		target := groups.OffsetResetTarget{Kind: input.Target}
		if input.Target == "timestamp" {
			millis, err := strconv.ParseInt(input.Timestamp.Value, 10, 64)
			if !input.Timestamp.IsSet || err != nil || millis < 0 {
				return nil, NewOffsetResetProblem(groups.ErrInvalidResetTarget, input.Group, input.Topic)
			}
			target.Timestamp = millis
		} else if input.Timestamp.IsSet {
			return nil, NewOffsetResetProblem(groups.ErrInvalidResetTarget, input.Group, input.Topic)
		}
		result, err := preview.Execute(ctx, input.Group, input.Topic, target)
		if err != nil {
			return nil, NewOffsetResetProblem(err, input.Group, input.Topic)
		}
		body := offsetResetPreviewBody{Group: result.Group, Topic: result.Topic, Partitions: make([]offsetResetPartition, 0, len(result.Partitions))}
		for _, partition := range result.Partitions {
			row := offsetResetPartition{Partition: partition.Partition, TargetOffset: strconv.FormatInt(partition.TargetOffset, 10)}
			if partition.CurrentOffset != nil {
				current := strconv.FormatInt(*partition.CurrentOffset, 10)
				row.CurrentOffset = &current
			}
			body.Partitions = append(body.Partitions, row)
		}
		return &PreviewOffsetResetOutput{Body: body}, nil
	})
}
