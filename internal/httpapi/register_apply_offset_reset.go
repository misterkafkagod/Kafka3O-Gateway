package httpapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type ApplyOffsetResetInput struct {
	Group string `path:"group" doc:"Consumer group ID"`
	Body  offsetResetApplyBody
}

type ApplyOffsetResetOutput struct {
	Body offsetResetAppliedBody
}

func RegisterApplyOffsetReset(api huma.API, apply groups.ApplyOffsetReset) {
	huma.Register(api, huma.Operation{
		OperationID: "apply-offset-reset",
		Method:      http.MethodPost,
		Path:        "/api/v1/consumer-groups/{group}/offset-reset",
		Summary:     "Commit previewed offsets for an inactive consumer group",
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ApplyOffsetResetInput) (*ApplyOffsetResetOutput, error) {
		requests := make([]groups.OffsetResetRequest, 0, len(input.Body.Partitions))
		for _, partition := range input.Body.Partitions {
			target, err := strconv.ParseInt(partition.TargetOffset, 10, 64)
			if err != nil {
				return nil, NewOffsetResetProblem(groups.ErrInvalidResetTarget, input.Group, input.Body.Topic)
			}
			request := groups.OffsetResetRequest{Partition: partition.Partition, TargetOffset: target}
			if partition.ExpectedOffset != nil {
				current, err := strconv.ParseInt(*partition.ExpectedOffset, 10, 64)
				if err != nil {
					return nil, NewOffsetResetProblem(groups.ErrInvalidResetTarget, input.Group, input.Body.Topic)
				}
				request.ExpectedOffset = &current
			}
			requests = append(requests, request)
		}
		result, err := apply.Execute(ctx, input.Group, input.Body.Topic, requests)
		if err != nil {
			return nil, NewOffsetResetProblem(err, input.Group, input.Body.Topic)
		}
		body := offsetResetAppliedBody{Group: result.Group, Topic: result.Topic, Partitions: make([]offsetResetCommitted, 0, len(result.Partitions))}
		for _, partition := range result.Partitions {
			body.Partitions = append(body.Partitions, offsetResetCommitted{Partition: partition.Partition, CommittedOffset: strconv.FormatInt(partition.TargetOffset, 10)})
		}
		return &ApplyOffsetResetOutput{Body: body}, nil
	})
}
