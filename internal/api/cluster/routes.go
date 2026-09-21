package cluster

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires the cluster commands onto humaAPI (FUNC-SPEC §8.7 C1, C2,
// C4; TECH-SPEC §6.2).
func Register(humaAPI huma.API, svc *cluster.Service) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-describe",
		Method:      http.MethodGet,
		Path:        "/v1/cluster",
		Summary:     "Describe the cluster",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C1"),
	}, describeCluster(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-broker-config",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/brokers/{brokerId}/config",
		Summary:     "Describe a broker's configuration",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C2"),
	}, describeBrokerConfig(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "cluster-health",
		Method:      http.MethodGet,
		Path:        "/v1/cluster/health",
		Summary:     "Cluster health summary",
		Tags:        []string{"Cluster"},
		Extensions:  commandExtension("C4"),
	}, clusterHealth(svc))
}

func describeCluster(svc *cluster.Service) func(context.Context, *DescribeClusterInput) (*DescribeClusterOutput, error) {
	return func(ctx context.Context, _ *DescribeClusterInput) (*DescribeClusterOutput, error) {
		info, err := svc.DescribeCluster(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeClusterOutput{Body: DescribeClusterBody{
			ClusterID:    info.ClusterID,
			ControllerID: info.ControllerID,
			Brokers:      toBrokerDTOs(info.Brokers),
		}}, nil
	}
}

func describeBrokerConfig(svc *cluster.Service) func(context.Context, *DescribeBrokerConfigInput) (*DescribeBrokerConfigOutput, error) {
	return func(ctx context.Context, in *DescribeBrokerConfigInput) (*DescribeBrokerConfigOutput, error) {
		configs, err := svc.DescribeBrokerConfig(ctx, in.BrokerID)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeBrokerConfigOutput{Body: DescribeBrokerConfigBody{
			BrokerID: in.BrokerID,
			Configs:  toBrokerConfigEntryDTOs(configs),
		}}, nil
	}
}

func clusterHealth(svc *cluster.Service) func(context.Context, *ClusterHealthInput) (*ClusterHealthOutput, error) {
	return func(ctx context.Context, _ *ClusterHealthInput) (*ClusterHealthOutput, error) {
		summary, err := svc.HealthSummary(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ClusterHealthOutput{Body: toHealthSummaryBody(summary)}, nil
	}
}
