package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

func RegisterReassignPartition(api huma.API, reassign partitions.ReassignPartition) {
	huma.Register(api, huma.Operation{
		OperationID:   "reassign-topic-partition",
		Method:        http.MethodPost,
		Path:          "/api/v1/topics/{topic}/partitions/{partitionId}/replicas",
		Summary:       "Request a manual partition replica assignment",
		DefaultStatus: http.StatusAccepted,
		Errors:        []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *ReassignPartitionInput) (*PartitionChangeOutput, error) {
		if len(input.Body.Replicas) == 0 {
			return nil, NewPartitionProblem(partitions.ErrInvalidChange)
		}
		if err := reassign.Execute(ctx, input.Topic, input.PartitionID, input.Body.Replicas); err != nil {
			return nil, NewPartitionProblem(err)
		}
		output := &PartitionChangeOutput{}
		output.Body.Topic = input.Topic
		output.Body.Status = "accepted"
		return output, nil
	})
}
