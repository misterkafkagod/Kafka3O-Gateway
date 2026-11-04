package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type ListConsumerGroupsOutput struct {
	Body []groups.ConsumerGroup
}

func RegisterListConsumerGroups(api huma.API, list groups.ListConsumerGroups) {
	huma.Register(api, huma.Operation{
		OperationID: "list-consumer-groups",
		Method:      http.MethodGet,
		Path:        "/api/v1/consumer-groups",
		Summary:     "Consumer groups and partition lag",
		Errors:      []int{http.StatusServiceUnavailable},
	}, func(ctx context.Context, _ *struct{}) (*ListConsumerGroupsOutput, error) {
		result, err := list.Execute(ctx)
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		return &ListConsumerGroupsOutput{Body: result}, nil
	})
}
