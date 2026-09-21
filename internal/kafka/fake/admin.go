package fake

import (
	"context"
	"sort"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// DescribeCluster returns the cluster id, controller, and seeded broker list
// (FUNC-SPEC §8.7 C1).
func (f *Fake) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	return invoke(f, ctx, "DescribeCluster", false, func() (kafka.ClusterInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		brokers := make([]kafka.Broker, len(f.model.brokers))
		copy(brokers, f.model.brokers)
		return kafka.ClusterInfo{
			ClusterID:    f.model.clusterID,
			ControllerID: f.model.controllerID,
			Brokers:      brokers,
		}, nil
	})
}

// DescribeBrokerConfigs returns one broker's seeded configuration properties
// (FUNC-SPEC §8.7 C2). Unknown brokerID → NotFound.
func (f *Fake) DescribeBrokerConfigs(ctx context.Context, brokerID int32) ([]kafka.ConfigEntry, error) {
	return invoke(f, ctx, "DescribeBrokerConfigs", false, func() ([]kafka.ConfigEntry, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.hasBroker(brokerID) {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "broker"}
		}
		configs := f.model.brokerConfigs[brokerID]
		out := make([]kafka.ConfigEntry, len(configs))
		copy(out, configs)
		return out, nil
	})
}

// Metadata returns every broker and topic, unfiltered (FUNC-SPEC §8.7 C4).
func (f *Fake) Metadata(ctx context.Context) (kafka.ClusterMetadata, error) {
	return invoke(f, ctx, "Metadata", false, func() (kafka.ClusterMetadata, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		brokers := make([]kafka.Broker, len(f.model.brokers))
		copy(brokers, f.model.brokers)
		topics := make([]kafka.Topic, 0, len(f.model.topics))
		for name, t := range f.model.topics {
			topics = append(topics, toDomainTopic(name, t))
		}
		sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
		return kafka.ClusterMetadata{Brokers: brokers, Topics: topics}, nil
	})
}

// ListTopics returns every seeded topic's summary, internal topics included
// (FUNC-SPEC §8.7 T1).
func (f *Fake) ListTopics(ctx context.Context) ([]kafka.TopicSummary, error) {
	return invoke(f, ctx, "ListTopics", false, func() ([]kafka.TopicSummary, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		out := make([]kafka.TopicSummary, 0, len(f.model.topics))
		for name, t := range f.model.topics {
			out = append(out, kafka.TopicSummary{
				Name:              name,
				Internal:          t.internal,
				PartitionCount:    len(t.partitions),
				ReplicationFactor: t.replicationFactor,
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out, nil
	})
}

// DescribeTopics returns one topic's partitions with leader/replicas/ISR;
// offsets are left zero — ListStartOffsets/ListEndOffsets fill them in, the
// same as the franz adapter's two-call contract (FUNC-SPEC §8.7 T2). Unknown
// topic → NotFound.
func (f *Fake) DescribeTopics(ctx context.Context, topic string) (kafka.Topic, error) {
	return invoke(f, ctx, "DescribeTopics", false, func() (kafka.Topic, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return kafka.Topic{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		return toDomainTopic(topic, t), nil
	})
}

// DescribeTopicConfigs returns one topic's seeded configuration properties
// (FUNC-SPEC §8.7 T2). Unknown topic → NotFound.
func (f *Fake) DescribeTopicConfigs(ctx context.Context, topic string) ([]kafka.ConfigEntry, error) {
	return invoke(f, ctx, "DescribeTopicConfigs", false, func() ([]kafka.ConfigEntry, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		out := make([]kafka.ConfigEntry, len(t.configs))
		copy(out, t.configs)
		return out, nil
	})
}

// ListStartOffsets returns the oldest (begin) offset per partition (FUNC-SPEC
// §8.7 T2, T4). Unknown topic → NotFound.
func (f *Fake) ListStartOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	return invoke(f, ctx, "ListStartOffsets", false, func() (map[int32]int64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		out := make(map[int32]int64, len(t.partitions))
		for i, p := range t.partitions {
			out[int32(i)] = p.beginOffset
		}
		return out, nil
	})
}

// ListEndOffsets returns the newest (end, exclusive) offset per partition
// (FUNC-SPEC §8.7 T2, T4). Unknown topic → NotFound.
func (f *Fake) ListEndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	return invoke(f, ctx, "ListEndOffsets", false, func() (map[int32]int64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		out := make(map[int32]int64, len(t.partitions))
		for i, p := range t.partitions {
			out[int32(i)] = p.beginOffset + int64(len(p.records))
		}
		return out, nil
	})
}

// ListOffsetsAfterMilli returns, per partition, the offset of the first
// record at or after millisecond, or the end offset if none qualifies
// (FUNC-SPEC §8.7 T4). Unknown topic → NotFound.
func (f *Fake) ListOffsetsAfterMilli(ctx context.Context, topic string, millisecond int64) (map[int32]int64, error) {
	return invoke(f, ctx, "ListOffsetsAfterMilli", false, func() (map[int32]int64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		target := time.UnixMilli(millisecond)
		out := make(map[int32]int64, len(t.partitions))
		for i, p := range t.partitions {
			offset := p.beginOffset + int64(len(p.records))
			for _, r := range p.records {
				if !r.Timestamp.Before(target) {
					offset = r.Offset
					break
				}
			}
			out[int32(i)] = offset
		}
		return out, nil
	})
}

// DescribeLogDirs returns every seeded per-partition, per-replica on-disk
// size for one topic (FUNC-SPEC §8.7 T3). Unknown topic → NotFound.
func (f *Fake) DescribeLogDirs(ctx context.Context, topic string) ([]kafka.LogDirReplica, error) {
	return invoke(f, ctx, "DescribeLogDirs", false, func() ([]kafka.LogDirReplica, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		var out []kafka.LogDirReplica
		for i, p := range t.partitions {
			for brokerID, entry := range p.logDir {
				out = append(out, kafka.LogDirReplica{
					Partition: int32(i),
					BrokerID:  brokerID,
					LogDir:    entry.dir,
					Bytes:     entry.bytes,
				})
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Partition != out[j].Partition {
				return out[i].Partition < out[j].Partition
			}
			return out[i].BrokerID < out[j].BrokerID
		})
		return out, nil
	})
}

// hasBroker reports whether brokerID was seeded. Callers must hold f.mu.
func (f *Fake) hasBroker(brokerID int32) bool {
	for _, b := range f.model.brokers {
		if b.ID == brokerID {
			return true
		}
	}
	return false
}

// toDomainTopic converts a fakeTopic into the port's Topic shape. Offsets are
// left zero to match the franz adapter's contract: DescribeTopics reports
// leader/replicas/ISR only, never offsets. Callers must hold f.mu.
func toDomainTopic(name string, t *fakeTopic) kafka.Topic {
	partitions := make([]kafka.Partition, len(t.partitions))
	for i, p := range t.partitions {
		partitions[i] = kafka.Partition{
			ID:       int32(i),
			Leader:   p.leader,
			Replicas: append([]int32(nil), p.replicas...),
			ISR:      append([]int32(nil), p.isr...),
		}
	}
	return kafka.Topic{
		Name:              name,
		Internal:          t.internal,
		ReplicationFactor: t.replicationFactor,
		Partitions:        partitions,
	}
}

// compile-time proof that Fake satisfies the Admin surface it implements so far.
var _ kafka.Admin = (*Fake)(nil)
