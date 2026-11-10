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
	logDirs ReadLogDirs
	health  ReadPartitionHealth
	offsets ReadEndOffsets
	timeout time.Duration
	now     func() time.Time
}

func NewGetMetrics(logDirs ReadLogDirs, health ReadPartitionHealth, offsets ReadEndOffsets, timeout time.Duration, now func() time.Time) GetMetrics {
	return getMetrics{logDirs: logDirs, health: health, offsets: offsets, timeout: timeout, now: now}
}

func (g getMetrics) Execute(ctx context.Context) ClusterMetrics {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	result := ClusterMetrics{ReadAt: g.now().UTC(), LogDirs: LogDirs{Dirs: []LogDir{}}}
	dirs, err := g.logDirs.Execute(ctx)
	if err != nil {
		result.LogDirs.Error = err.Error()
		result.LargestTopics.Error = err.Error()
	}
	if dirs != nil {
		result.LogDirs.Dirs = slices.Clone(dirs)
		slices.SortFunc(result.LogDirs.Dirs, CompareLogDirs)
	}
	result.LargestTopics.Topics = LargestTopics(dirs)

	topics, err := g.health.Execute(ctx)
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
	offsets, err := g.offsets.Execute(ctx, names)
	if err != nil {
		result.EndOffsets = EndOffsets{Error: err.Error(), Topics: []TopicEndOffset{}}
	} else {
		result.EndOffsets = EndOffsets{Topics: offsets}
	}
	return result
}
