package cluster

import (
	"context"
	"regexp"
	"sort"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Throughput's `seconds` bound (FUNC-SPEC §8.8 C10 row): default 5, ceiling 60.
const (
	defaultThroughputSeconds = 5
	maxThroughputSeconds     = 60
)

// resolveIntBound applies def when v is zero, and reports BoundExceeded
// (naming the offending field) when v exceeds ceil (FUNC-SPEC §8.8) — the
// same shape internal/service/message's own resolveIntBound uses.
func resolveIntBound(name string, v, def, ceil int) (int, error) {
	if v == 0 {
		return def, nil
	}
	if v > ceil {
		return 0, &core.PolicyError{Code: core.BoundExceeded, Message: name + " exceeds the configured ceiling"}
	}
	return v, nil
}

// Quorum returns the KRaft quorum's current status (FUNC-SPEC §8.7 C6). A
// ZooKeeper-mode cluster's own NotFound error, unchanged (from
// Admin.DescribeQuorum's *Error{Kind: KindUnsupported}).
func (s *Service) Quorum(ctx context.Context) (kafka.QuorumStatus, error) {
	return s.admin.DescribeQuorum(ctx)
}

// Reassignments returns every partition cluster-wide with a reassignment
// currently in progress (FUNC-SPEC §8.7 C7).
func (s *Service) Reassignments(ctx context.Context) ([]kafka.PartitionReassignment, error) {
	return s.admin.ListReassignments(ctx)
}

// LogDirs returns every broker's log directory usage, cluster-wide
// (FUNC-SPEC §8.7 C8).
func (s *Service) LogDirs(ctx context.Context) ([]kafka.BrokerLogDir, error) {
	return s.admin.DescribeAllLogDirs(ctx)
}

// ThroughputItem is one topic's message rate over a Throughput sample
// (FUNC-SPEC §8.7 C10).
type ThroughputItem struct {
	Topic             string
	MessagesPerSecond float64
	StartEndOffsets   map[int32]int64
	FinishEndOffsets  map[int32]int64
}

// Throughput derives messages-per-second for topic (or, when topic is
// empty, every non-internal topic) from two end-offset snapshots seconds
// apart (FUNC-SPEC §8.7 C10; §8.8 default 5, ceiling 60). No state
// persists between calls — each call takes its own two snapshots.
func (s *Service) Throughput(ctx context.Context, topic string, seconds int) ([]ThroughputItem, int, error) {
	seconds, err := resolveIntBound("seconds", seconds, defaultThroughputSeconds, maxThroughputSeconds)
	if err != nil {
		return nil, 0, err
	}

	topics, err := s.throughputTopics(ctx, topic)
	if err != nil {
		return nil, 0, err
	}

	start, err := s.snapshotEndOffsets(ctx, topics)
	if err != nil {
		return nil, 0, err
	}
	s.sleep(time.Duration(seconds) * time.Second)
	finish, err := s.snapshotEndOffsets(ctx, topics)
	if err != nil {
		return nil, 0, err
	}

	items := make([]ThroughputItem, len(topics))
	for i, name := range topics {
		var total int64
		for p, end := range finish[name] {
			total += end - start[name][p]
		}
		items[i] = ThroughputItem{
			Topic: name, MessagesPerSecond: float64(total) / float64(seconds),
			StartEndOffsets: start[name], FinishEndOffsets: finish[name],
		}
	}
	return items, seconds, nil
}

// throughputTopics resolves Throughput's scope: topic alone when given,
// else every non-internal topic, sorted for a stable response.
func (s *Service) throughputTopics(ctx context.Context, topic string) ([]string, error) {
	if topic != "" {
		return []string{topic}, nil
	}
	all, err := s.admin.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range all {
		if !t.Internal {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// snapshotEndOffsets takes one end-offset reading per partition for every
// named topic.
func (s *Service) snapshotEndOffsets(ctx context.Context, topics []string) (map[string]map[int32]int64, error) {
	out := make(map[string]map[int32]int64, len(topics))
	for _, name := range topics {
		end, err := s.admin.ListEndOffsets(ctx, name)
		if err != nil {
			return nil, err
		}
		out[name] = end
	}
	return out, nil
}

// ExportTopic is one topic's exported definition (FUNC-SPEC §8.7 C11):
// Configs carries only overrides — keys whose current Source is Dynamic —
// never the full configuration set.
type ExportTopic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// Export snapshots every topic matching pattern (or every topic, when
// pattern is empty) as a declarative definition C12 can later apply
// (FUNC-SPEC §8.7 C11). An invalid pattern reports
// *core.PolicyError{Code: core.InvalidRegex}, the same as T1's own
// `?pattern=` (TECH-SPEC B7).
func (s *Service) Export(ctx context.Context, pattern string) ([]ExportTopic, error) {
	var re *regexp.Regexp
	if pattern != "" {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, &core.PolicyError{Code: core.InvalidRegex, Message: err.Error()}
		}
		re = compiled
	}

	all, err := s.admin.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	var matched []kafka.TopicSummary
	for _, t := range all {
		if t.Internal {
			continue
		}
		if re != nil && !re.MatchString(t.Name) {
			continue
		}
		matched = append(matched, t)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	out := make([]ExportTopic, len(matched))
	for i, t := range matched {
		configs, err := s.admin.DescribeTopicConfigs(ctx, t.Name)
		if err != nil {
			return nil, err
		}
		overrides := map[string]string{}
		for _, c := range configs {
			if c.Source == kafka.SourceDynamic {
				overrides[c.Name] = c.Value
			}
		}
		out[i] = ExportTopic{
			Name: t.Name, Partitions: int32(t.PartitionCount), ReplicationFactor: int16(t.ReplicationFactor),
			Configs: overrides,
		}
	}
	return out, nil
}
