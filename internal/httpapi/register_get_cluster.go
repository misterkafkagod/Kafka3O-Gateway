package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
)

type GetClusterOutput struct {
	Body cluster.Cluster
}

// RegisterGetCluster registers GET /api/v1/cluster: the cluster ID, controller and brokers,
// or 503 cluster_unreachable when Kafka cannot be reached.
func RegisterGetCluster(api huma.API, getCluster cluster.GetCluster) {
	huma.Register(api, huma.Operation{
		OperationID: "get-cluster",
		Method:      http.MethodGet,
		Path:        "/api/v1/cluster",
		Summary:     "Cluster identity and brokers",
		Errors:      []int{http.StatusServiceUnavailable},
	}, func(ctx context.Context, _ *struct{}) (*GetClusterOutput, error) {
		c, err := getCluster.Execute(ctx)
		if err != nil {
			return nil, NewClusterUnreachable(err)
		}
		return &GetClusterOutput{Body: c}, nil
	})
}
