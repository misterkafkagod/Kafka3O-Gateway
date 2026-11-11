package metrics

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
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

// Execute reads the sections concurrently under one timeout; each writes only its own fields of the result.
func (g getMetrics) Execute(ctx context.Context) ClusterMetrics {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	result := ClusterMetrics{ReadAt: g.now().UTC(), LogDirs: LogDirs{Dirs: []LogDir{}}, Quorum: Quorum{Replicas: []QuorumReplica{}}}
	var wait sync.WaitGroup
	wait.Go(func() {
		if quorum, err := g.sources.Quorum.Execute(ctx); err != nil {
			result.Quorum.Error = err.Error()
		} else {
			result.Quorum = quorum
		}
	})
	wait.Go(func() {
		if brokers, err := g.sources.Brokers.Execute(ctx); err != nil {
			result.Brokers = Brokers{Error: err.Error(), Brokers: []BrokerLeaders{}}
		} else {
			result.Brokers = Brokers{Brokers: brokers}
		}
	})
	wait.Go(func() {
		if consumerGroups, err := g.sources.Groups.Execute(ctx); err != nil {
			result.Groups = Groups{Error: err.Error(), States: []StateCount{}, Groups: []GroupLag{}}
		} else {
			result.Groups = SummarizeConsumerGroups(consumerGroups)
		}
	})
	wait.Go(func() {
		connectors, err := g.sources.Connect.Execute(ctx)
		switch {
		case errors.Is(err, connect.ErrConnectNotConfigured):
			result.Connect = Connect{NotConfigured: true, Connectors: []StateCount{}, Tasks: []StateCount{}, Failures: []ConnectFailure{}}
		case err != nil:
			result.Connect = Connect{Error: err.Error(), Connectors: []StateCount{}, Tasks: []StateCount{}, Failures: []ConnectFailure{}}
		default:
			result.Connect = SummarizeConnect(connectors)
		}
	})
	wait.Go(func() {
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
	})
	wait.Go(func() {
		topics, err := g.sources.Health.Execute(ctx)
		if err != nil {
			result.Partitions = Partitions{Error: err.Error(), UnreadableTopics: []string{}}
			result.EndOffsets = EndOffsets{Error: err.Error(), Topics: []TopicEndOffset{}}
			return
		}
		result.Partitions = SummarizePartitionHealth(topics)
		names := make([]string, 0, len(topics))
		for _, topic := range topics {
			if topic.Error == "" {
				names = append(names, topic.Topic)
			}
		}
		if offsets, err := g.sources.Offsets.Execute(ctx, names); err != nil {
			result.EndOffsets = EndOffsets{Error: err.Error(), Topics: []TopicEndOffset{}}
		} else {
			result.EndOffsets = EndOffsets{Topics: offsets}
		}
	})
	wait.Wait()
	return result
}
