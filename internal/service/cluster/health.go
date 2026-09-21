package cluster

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// maxAffected caps HealthSummary.Affected at 1 000 entries (TECH-SPEC C11).
const maxAffected = 1000

// Affected-partition issue values (FUNC-SPEC §8.7 C4).
const (
	IssueOffline            = "offline"
	IssueUnderReplicated    = "under_replicated"
	IssueNonPreferredLeader = "non_preferred_leader"
)

// AffectedPartition is one partition contributing one issue to a
// HealthSummary (FUNC-SPEC §8.7 C4).
type AffectedPartition struct {
	Topic     string
	Partition int32
	Issue     string
}

// HealthSummary is the cluster health snapshot (FUNC-SPEC §8.7 C4). The
// counts are exact; Affected is capped at 1 000 entries with Truncated set
// once more partitions have an issue than fit (TECH-SPEC C11).
type HealthSummary struct {
	BrokersOnline                int
	TopicsTotal                  int
	PartitionsTotal              int
	PartitionsOffline            int
	PartitionsUnderReplicated    int
	PartitionsNonPreferredLeader int
	Affected                     []AffectedPartition
	Truncated                    bool
}

// HealthSummary computes broker/topic/partition counts and every partition
// that is offline (no leader), under-replicated (ISR smaller than its
// replica set), or led by other than its preferred (first) replica
// (FUNC-SPEC §8.7 C4). A partition can contribute more than one issue.
// Topics and partitions are walked in a stable (name, id) order so which
// entries survive the Affected cap is deterministic.
func (s *Service) HealthSummary(ctx context.Context) (HealthSummary, error) {
	md, err := s.admin.Metadata(ctx)
	if err != nil {
		return HealthSummary{}, err
	}

	topics := append([]kafka.Topic(nil), md.Topics...)
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	summary := HealthSummary{
		BrokersOnline: len(md.Brokers),
		TopicsTotal:   len(topics),
	}
	for _, t := range topics {
		partitions := append([]kafka.Partition(nil), t.Partitions...)
		sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
		for _, p := range partitions {
			summary.count(t.Name, p)
		}
	}
	return summary, nil
}

// count classifies one partition, updating totals and appending to Affected
// (subject to the cap). Callers must have already stably ordered partitions.
func (h *HealthSummary) count(topic string, p kafka.Partition) {
	h.PartitionsTotal++

	offline := p.Leader == -1
	underReplicated := len(p.ISR) < len(p.Replicas)
	nonPreferred := !offline && len(p.Replicas) > 0 && p.Leader != p.Replicas[0]

	if offline {
		h.PartitionsOffline++
		h.addAffected(topic, p.ID, IssueOffline)
	}
	if underReplicated {
		h.PartitionsUnderReplicated++
		h.addAffected(topic, p.ID, IssueUnderReplicated)
	}
	if nonPreferred {
		h.PartitionsNonPreferredLeader++
		h.addAffected(topic, p.ID, IssueNonPreferredLeader)
	}
}

// addAffected appends one entry, or sets Truncated once the cap is reached.
func (h *HealthSummary) addAffected(topic string, partition int32, issue string) {
	if len(h.Affected) >= maxAffected {
		h.Truncated = true
		return
	}
	h.Affected = append(h.Affected, AffectedPartition{Topic: topic, Partition: partition, Issue: issue})
}
