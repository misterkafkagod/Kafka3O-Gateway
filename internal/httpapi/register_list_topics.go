package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ListTopicsOutput struct {
	Body []topics.Topic
}

// RegisterListTopics registers GET /api/v1/topics: every topic with partitions, replication factor,
// internal flag and approximate message count, or 503 cluster_unreachable when Kafka cannot be reached.
func RegisterListTopics(api huma.API, listTopics topics.ListTopics) {
	huma.Register(api, huma.Operation{
		OperationID: "list-topics",
		Method:      http.MethodGet,
		Path:        "/api/v1/topics",
		Summary:     "Topics with approximate message counts",
		Errors:      []int{http.StatusServiceUnavailable},
	}, func(ctx context.Context, _ *struct{}) (*ListTopicsOutput, error) {
		t, err := listTopics.Execute(ctx)
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		return &ListTopicsOutput{Body: t}, nil
	})
}
