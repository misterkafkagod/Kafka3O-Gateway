// Package cluster implements the cluster-administration commands (FUNC-SPEC
// §8.7 C1, C2, C4). Service holds only kafka.Admin — it never reads or
// writes messages (TECH-SPEC I2: "a service holding a role it never calls is
// a defect").
package cluster

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Service implements the cluster commands over a kafka.Admin.
type Service struct {
	admin kafka.Admin
}

// New builds a Service over admin.
func New(admin kafka.Admin) *Service {
	return &Service{admin: admin}
}

// DescribeCluster returns the cluster id, controller, and broker list (C1).
func (s *Service) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	return s.admin.DescribeCluster(ctx)
}

// DescribeBrokerConfig returns one broker's configuration properties, every
// sensitive entry's value blanked regardless of what the adapter already did
// — the service layer is the last line of defence for this guarantee
// (FUNC-SPEC §8.2 sensitive; C2).
func (s *Service) DescribeBrokerConfig(ctx context.Context, brokerID int32) ([]kafka.ConfigEntry, error) {
	configs, err := s.admin.DescribeBrokerConfigs(ctx, brokerID)
	if err != nil {
		return nil, err
	}
	return maskSensitive(configs), nil
}

// maskSensitive returns a copy of configs with every sensitive entry's value
// blanked.
func maskSensitive(configs []kafka.ConfigEntry) []kafka.ConfigEntry {
	out := make([]kafka.ConfigEntry, len(configs))
	for i, c := range configs {
		if c.IsSensitive {
			c.Value = ""
		}
		out[i] = c
	}
	return out
}
