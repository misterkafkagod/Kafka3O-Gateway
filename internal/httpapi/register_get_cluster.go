package httpapi

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
)

type GetClusterOutput struct {
	Body cluster.Cluster
}

// RegisterGetCluster registers GET /api/v1/cluster: the cluster ID, controller and brokers.
func RegisterGetCluster(api huma.API, getCluster cluster.GetCluster) {
	huma.Register(api, huma.Operation{
		OperationID: "get-cluster",
		Method:      "GET",
		Path:        "/api/v1/cluster",
		Summary:     "Cluster identity and brokers",
	}, func(ctx context.Context, _ *struct{}) (*GetClusterOutput, error) {
		c, err := getCluster.Execute(ctx)
		if err != nil {
			return nil, err
		}
		return &GetClusterOutput{Body: c}, nil
	})
}
