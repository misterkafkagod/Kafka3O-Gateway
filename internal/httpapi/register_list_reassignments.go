package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

func RegisterListReassignments(api huma.API, list partitions.ListReassignments) {
	huma.Register(api, huma.Operation{
		OperationID: "list-topic-reassignments",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics/{topic}/reassignments",
		Summary:     "List active partition reassignments",
		Errors:      []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, func(ctx context.Context, input *TopicConfigurationInput) (*ReassignmentsOutput, error) {
		reassignments, err := list.Execute(ctx, input.Topic)
		if err != nil {
			return nil, NewPartitionProblem(err)
		}
		return &ReassignmentsOutput{Body: reassignments}, nil
	})
}
