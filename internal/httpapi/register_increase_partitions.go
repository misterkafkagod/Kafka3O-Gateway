package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

func RegisterIncreasePartitions(api huma.API, increase partitions.IncreasePartitionCount) {
	huma.Register(api, huma.Operation{
		OperationID:   "increase-topic-partitions",
		Method:        http.MethodPost,
		Path:          "/api/v1/topics/{topic}/partitions",
		Summary:       "Increase a topic's partition count",
		DefaultStatus: http.StatusAccepted,
		Errors:        []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *IncreasePartitionsInput) (*PartitionChangeOutput, error) {
		if err := increase.Execute(ctx, input.Topic, input.Body.Count); err != nil {
			return nil, NewPartitionProblem(err)
		}
		output := &PartitionChangeOutput{}
		output.Body.Topic = input.Topic
		output.Body.Status = "accepted"
		return output, nil
	})
}
