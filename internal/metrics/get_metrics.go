package metrics

import (
	"context"
	"slices"
	"time"
)

// GetMetrics reads the cluster's metrics from Kafka; a section Kafka cannot answer carries its error, never zeros.
type GetMetrics interface {
	Execute(ctx context.Context) ClusterMetrics
}

type getMetrics struct {
	sources Sources
	timeout time.Duration
	now     func() time.Time
}

func NewGetMetrics(sources Sources, timeout time.Duration, now func() time.Time) GetMetrics {
	return getMetrics{sources: sources, timeout: timeout, now: now}
}

func (g getMetrics) Execute(ctx context.Context) ClusterMetrics {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	result := ClusterMetrics{ReadAt: g.now().UTC(), LogDirs: LogDirs{Dirs: []LogDir{}}, Quorum: Quorum{Replicas: []QuorumReplica{}}}
	if quorum, err := g.sources.Quorum.Execute(ctx); err != nil {
		result.Quorum.Error = err.Error()
	} else {
		result.Quorum = quorum
	}
	if brokers, err := g.sources.Brokers.Execute(ctx); err != nil {
		result.Brokers = Brokers{Error: err.Error(), Brokers: []BrokerLeaders{}}
	} else {
		result.Brokers = Brokers{Brokers: brokers}
	}
	dirs, err := g.sources.LogDirs.Execute(ctx)
	if err != nil {
		result.LogDirs.Error = err.Error()
		result.LargestTopics.Error = err.Error()
	}
	if dirs != nil {
		result.LogDirs.Dirs = slices.Clone(dirs)
		slices.SortFunc(result.LogDirs.Dirs, CompareLogDirs)
	}
	result.LargestTopics.Topics = LargestTopics(dirs)

	topics, err := g.sources.Health.Execute(ctx)
	if err != nil {
		result.Partitions = Partitions{Error: err.Error(), UnreadableTopics: []string{}}
		result.EndOffsets = EndOffsets{Error: err.Error(), Topics: []TopicEndOffset{}}
		return result
	}
	result.Partitions = SummarizePartitionHealth(topics)
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		if topic.Error == "" {
			names = append(names, topic.Topic)
		}
	}
	offsets, err := g.sources.Offsets.Execute(ctx, names)
	if err != nil {
		result.EndOffsets = EndOffsets{Error: err.Error(), Topics: []TopicEndOffset{}}
	} else {
		result.EndOffsets = EndOffsets{Topics: offsets}
	}
	return result
}
