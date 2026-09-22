// Package cluster implements the cluster-administration commands (FUNC-SPEC
// §8.7 C1-C12). Service holds only kafka.Admin — it never reads or writes
// messages (TECH-SPEC I2: "a service holding a role it never calls is a
// defect").
package cluster

import (
	"context"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Service implements the cluster commands over a kafka.Admin.
type Service struct {
	admin kafka.Admin
	// auditor records C5, C9, C12's ATTEMPT/RESULT audit trail (FUNC-SPEC
	// §8.5) — C1-C4, C6-C8, C10, C11 are reads and stay unaudited
	// (FUNC-SPEC §8.5 scope).
	auditor    *audit.Auditor
	newEventID func() string
	now        func() time.Time
	// sleep waits out C10's `seconds` window between its two end-offset
	// snapshots; injected so a fixed-clock test never actually waits
	// (TECH-SPEC §4.9).
	sleep func(time.Duration)
	// runner gates C5, C9, C12 (FUNC-SPEC §9.1 nodes F-K3): an
	// operator-only caller, plus (all three are in FUNC-SPEC §5.6) F3's
	// per-operation switch.
	runner core.Runner
}

// New builds a Service over admin, auditor, and runner.
func New(admin kafka.Admin, auditor *audit.Auditor, runner core.Runner) *Service {
	return &Service{
		admin: admin, auditor: auditor, newEventID: audit.NewEventID,
		now: time.Now, sleep: time.Sleep, runner: runner,
	}
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
