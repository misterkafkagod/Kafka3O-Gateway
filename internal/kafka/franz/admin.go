package franz

import (
	"context"
	"sort"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// DescribeCluster returns the cluster id, controller, and broker list
// (FUNC-SPEC §8.7 C1) via kadm.BrokerMetadata.
func (c *Client) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	md, err := c.kadm.BrokerMetadata(ctx)
	if err != nil {
		return kafka.ClusterInfo{}, wrapErr("", err)
	}
	brokers := make([]kafka.Broker, len(md.Brokers))
	for i, b := range md.Brokers {
		brokers[i] = toDomainBroker(b)
	}
	return kafka.ClusterInfo{ClusterID: md.Cluster, ControllerID: md.Controller, Brokers: brokers}, nil
}

// DescribeBrokerConfigs returns one broker's configuration properties via
// kadm.DescribeBrokerConfigs (FUNC-SPEC §8.7 C2).
func (c *Client) DescribeBrokerConfigs(ctx context.Context, brokerID int32) ([]kafka.ConfigEntry, error) {
	rcs, err := c.kadm.DescribeBrokerConfigs(ctx, brokerID)
	if err != nil {
		return nil, wrapErr("broker", err)
	}
	rc, err := rcs.On(strconv.Itoa(int(brokerID)), nil)
	if err != nil {
		return nil, wrapErr("broker", err)
	}
	if rc.Err != nil {
		return nil, wrapErr("broker", rc.Err)
	}
	return toConfigEntries(rc.Configs), nil
}

// Metadata returns every broker and topic, unfiltered, via kadm.Metadata
// (FUNC-SPEC §8.7 C4).
func (c *Client) Metadata(ctx context.Context) (kafka.ClusterMetadata, error) {
	md, err := c.kadm.Metadata(ctx)
	if err != nil {
		return kafka.ClusterMetadata{}, wrapErr("", err)
	}
	brokers := make([]kafka.Broker, len(md.Brokers))
	for i, b := range md.Brokers {
		brokers[i] = toDomainBroker(b)
	}
	topics := make([]kafka.Topic, 0, len(md.Topics))
	for _, td := range md.Topics.Sorted() {
		if td.Err != nil {
			continue
		}
		topics = append(topics, toDomainTopicDetail(td))
	}
	return kafka.ClusterMetadata{Brokers: brokers, Topics: topics}, nil
}

// ListTopics returns every topic's summary via kadm.ListTopicsWithInternal,
// internal topics included (FUNC-SPEC §8.7 T1).
func (c *Client) ListTopics(ctx context.Context) ([]kafka.TopicSummary, error) {
	tds, err := c.kadm.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, wrapErr("", err)
	}
	out := make([]kafka.TopicSummary, 0, len(tds))
	for _, td := range tds.Sorted() {
		if td.Err != nil {
			continue
		}
		out = append(out, kafka.TopicSummary{
			Name:              td.Topic,
			Internal:          td.IsInternal,
			PartitionCount:    len(td.Partitions),
			ReplicationFactor: td.Partitions.NumReplicas(),
		})
	}
	return out, nil
}

// DescribeTopics returns one topic's partitions (leader, replicas, ISR) via
// kadm.ListTopicsWithInternal (FUNC-SPEC §8.7 T2). Unknown topic → NotFound.
func (c *Client) DescribeTopics(ctx context.Context, topic string) (kafka.Topic, error) {
	tds, err := c.kadm.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return kafka.Topic{}, wrapErr("topic", err)
	}
	td, ok := tds[topic]
	if !ok {
		return kafka.Topic{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
	}
	if td.Err != nil {
		return kafka.Topic{}, wrapErr("topic", td.Err)
	}
	return toDomainTopicDetail(td), nil
}

// DescribeTopicConfigs returns one topic's configuration properties via
// kadm.DescribeTopicConfigs (FUNC-SPEC §8.7 T2). Unknown topic → NotFound.
func (c *Client) DescribeTopicConfigs(ctx context.Context, topic string) ([]kafka.ConfigEntry, error) {
	rcs, err := c.kadm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	if rc.Err != nil {
		return nil, wrapErr("topic", rc.Err)
	}
	return toConfigEntries(rc.Configs), nil
}

// ListStartOffsets returns the oldest (begin) offset per partition via
// kadm.ListStartOffsets (FUNC-SPEC §8.7 T2, T4). Unknown topic → NotFound.
func (c *Client) ListStartOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	lo, err := c.kadm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	return toOffsetMap(topic, lo)
}

// ListEndOffsets returns the newest (end, exclusive) offset per partition via
// kadm.ListEndOffsets (FUNC-SPEC §8.7 T2, T4). Unknown topic → NotFound.
func (c *Client) ListEndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	lo, err := c.kadm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	return toOffsetMap(topic, lo)
}

// ListOffsetsAfterMilli returns, per partition, the offset of the first
// record at or after millisecond via kadm.ListOffsetsAfterMilli (FUNC-SPEC
// §8.7 T4). Unknown topic → NotFound.
func (c *Client) ListOffsetsAfterMilli(ctx context.Context, topic string, millisecond int64) (map[int32]int64, error) {
	lo, err := c.kadm.ListOffsetsAfterMilli(ctx, millisecond, topic)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	return toOffsetMap(topic, lo)
}

// DescribeLogDirs returns per-partition, per-replica on-disk byte sizes for
// one topic via kadm.DescribeAllLogDirs (FUNC-SPEC §8.7 T3). Unknown topic →
// NotFound only surfaces once the caller inspects the (empty) result against
// DescribeTopics, matching a broker that simply reports no log dirs for it.
func (c *Client) DescribeLogDirs(ctx context.Context, topic string) ([]kafka.LogDirReplica, error) {
	var set kadm.TopicsSet
	set.Add(topic)
	dirs, err := c.kadm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return nil, wrapErr("topic", err)
	}
	var out []kafka.LogDirReplica
	for _, logDirsByPath := range dirs {
		for _, d := range logDirsByPath {
			if d.Err != nil {
				continue
			}
			partitions, ok := d.Topics[topic]
			if !ok {
				continue
			}
			for _, p := range partitions {
				out = append(out, kafka.LogDirReplica{
					Partition: p.Partition,
					BrokerID:  d.Broker,
					LogDir:    d.Dir,
					Bytes:     p.Size,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].BrokerID < out[j].BrokerID
	})
	return out, nil
}

// toDomainBroker converts one kadm broker into the port's Broker shape.
func toDomainBroker(b kadm.BrokerDetail) kafka.Broker {
	var rack string
	if b.Rack != nil {
		rack = *b.Rack
	}
	return kafka.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack}
}

// toDomainTopicDetail converts a kadm.TopicDetail into the port's Topic
// shape. Offsets are left zero — ListStartOffsets/ListEndOffsets fill them
// in (FUNC-SPEC §8.7 T2).
func toDomainTopicDetail(td kadm.TopicDetail) kafka.Topic {
	partitions := make([]kafka.Partition, 0, len(td.Partitions))
	for _, pd := range td.Partitions.Sorted() {
		partitions = append(partitions, kafka.Partition{
			ID:       pd.Partition,
			Leader:   pd.Leader,
			Replicas: append([]int32(nil), pd.Replicas...),
			ISR:      append([]int32(nil), pd.ISR...),
		})
	}
	return kafka.Topic{
		Name:              td.Topic,
		Internal:          td.IsInternal,
		ReplicationFactor: td.Partitions.NumReplicas(),
		Partitions:        partitions,
	}
}

// toConfigEntries converts kadm configs into the port's ConfigEntry shape,
// blanking the value of every sensitive entry (FUNC-SPEC §8.2 sensitive).
func toConfigEntries(configs []kadm.Config) []kafka.ConfigEntry {
	out := make([]kafka.ConfigEntry, len(configs))
	for i, c := range configs {
		value := c.MaybeValue()
		if c.Sensitive {
			value = ""
		}
		out[i] = kafka.ConfigEntry{
			Name:        c.Key,
			Value:       value,
			Source:      normalizeConfigSource(c.Source),
			IsSensitive: c.Sensitive,
		}
	}
	return out
}

// toOffsetMap extracts one topic's per-partition offsets from a kadm listing.
// A topic absent from the listing, or present only via the special -1
// partition kadm adds for an unknown topic, maps to NotFound.
func toOffsetMap(topic string, lo kadm.ListedOffsets) (map[int32]int64, error) {
	partitions, ok := lo[topic]
	if !ok {
		return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
	}
	out := make(map[int32]int64, len(partitions))
	for p, o := range partitions {
		if o.Err != nil {
			return nil, wrapErr("topic", o.Err)
		}
		if p < 0 {
			continue
		}
		out[p] = o.Offset
	}
	return out, nil
}

// compile-time proof that Client satisfies the Admin surface it implements so far.
var _ kafka.Admin = (*Client)(nil)
