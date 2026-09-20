package kafka

import "context"

// Admin is the cluster-administration surface (FUNC-SPEC §8.1: C*, T*, G*, S*
// commands). Methods are added phase by phase in catalog order; every method
// honours ctx cancellation and returns *Error on failure (TECH-SPEC L2, L3).
type Admin interface {
	// DescribeCluster returns the cluster id, controller, and broker list (C1).
	DescribeCluster(ctx context.Context) (ClusterInfo, error)
}

// Consumer is the message-reading surface (FUNC-SPEC §8.1: M1–M4, M8 source,
// T4, C10). It uses manual partition assignment — no group, no commits
// (FUNC-SPEC O4). Methods arrive with Phase 3 (Task 3.1).
type Consumer interface{}

// Producer is the message-writing surface (FUNC-SPEC §8.1: M5–M8 target and
// the F5 audit sink). Methods arrive with Phase 5 (Task 5.1).
type Producer interface{}
