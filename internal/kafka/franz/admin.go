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

// ListGroups returns every consumer group's state, protocol type, and
// member count (FUNC-SPEC §8.7 G1): kadm.ListGroups first, for cheap
// server-side state filtering, then kadm.DescribeGroups over exactly that
// filtered set for each group's member count (ListGroups' own response
// carries no member count).
func (c *Client) ListGroups(ctx context.Context, states ...string) ([]kafka.GroupSummary, error) {
	listed, err := c.kadm.ListGroups(ctx, states...)
	if err != nil {
		return nil, wrapErr("", err)
	}
	if len(listed) == 0 {
		return nil, nil
	}

	described, err := c.kadm.DescribeGroups(ctx, listed.Groups()...)
	if err != nil {
		return nil, wrapErr("", err)
	}

	out := make([]kafka.GroupSummary, 0, len(listed))
	for _, g := range listed.Sorted() {
		memberCount := 0
		if d, ok := described[g.Group]; ok && d.Err == nil {
			memberCount = len(d.Members)
		}
		out = append(out, kafka.GroupSummary{
			ID: g.Group, State: g.State, ProtocolType: g.ProtocolType, MemberCount: memberCount,
		})
	}
	return out, nil
}

// DescribeGroups returns full detail for groupIDs, or every group in the
// cluster when groupIDs is empty, via kadm.DescribeGroups (FUNC-SPEC §8.7
// G2, G3). Given explicit ids, any missing or errored one fails the whole
// call as NotFound; given none, a group kadm reports an error for is
// silently omitted from the best-effort sweep.
func (c *Client) DescribeGroups(ctx context.Context, groupIDs ...string) ([]kafka.Group, error) {
	described, err := c.kadm.DescribeGroups(ctx, groupIDs...)
	if err != nil {
		return nil, wrapErr("", err)
	}

	if len(groupIDs) > 0 {
		out := make([]kafka.Group, len(groupIDs))
		for i, id := range groupIDs {
			d, err := described.On(id, nil)
			if err != nil {
				return nil, wrapErr("group", err)
			}
			if d.Err != nil {
				return nil, wrapErr("group", d.Err)
			}
			out[i] = toDomainGroup(d)
		}
		return out, nil
	}

	out := make([]kafka.Group, 0, len(described))
	for _, d := range described.Sorted() {
		if d.Err != nil {
			continue
		}
		out = append(out, toDomainGroup(d))
	}
	return out, nil
}

// FetchGroupOffsets returns groupID's committed offset per topic partition
// (FUNC-SPEC §8.7 G2 offsets[], G3 lag): kadm.DescribeGroups first, since
// kadm.FetchOffsets alone does not distinguish an unknown group from one
// with no committed offsets yet, then kadm.FetchOffsets for the values.
func (c *Client) FetchGroupOffsets(ctx context.Context, groupID string) (map[kafka.TopicPartition]int64, error) {
	described, err := c.kadm.DescribeGroups(ctx, groupID)
	if err != nil {
		return nil, wrapErr("group", err)
	}
	if _, err := described.On(groupID, nil); err != nil {
		return nil, wrapErr("group", err)
	}

	responses, err := c.kadm.FetchOffsets(ctx, groupID)
	if err != nil {
		return nil, wrapErr("group", err)
	}
	out := make(map[kafka.TopicPartition]int64)
	for topic, partitions := range responses {
		for partition, o := range partitions {
			if o.Err != nil {
				continue
			}
			out[kafka.TopicPartition{Topic: topic, Partition: partition}] = o.At
		}
	}
	return out, nil
}

// toDomainGroup converts a kadm.DescribedGroup into the port's Group shape,
// extracting each member's assigned topic-partitions from its consumer
// assignment (kmsg.ConsumerMemberAssignment) when present.
func toDomainGroup(d kadm.DescribedGroup) kafka.Group {
	members := make([]kafka.GroupMember, len(d.Members))
	for i, m := range d.Members {
		var assignments []kafka.TopicPartition
		if consumer, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range consumer.Topics {
				for _, p := range t.Partitions {
					assignments = append(assignments, kafka.TopicPartition{Topic: t.Topic, Partition: p})
				}
			}
		}
		members[i] = kafka.GroupMember{
			MemberID: m.MemberID, ClientID: m.ClientID, Host: m.ClientHost, Assignments: assignments,
		}
	}
	return kafka.Group{
		ID: d.Group, State: d.State, ProtocolType: d.ProtocolType,
		CoordinatorID: d.Coordinator.NodeID, Members: members,
	}
}

// CreateTopics creates every spec — or, when validateOnly, asks the broker
// to check them without creating anything — one kadm call per spec, since
// kadm.CreateTopics applies a single partitions/replicationFactor/configs
// triple to every name it's given, and specs may each differ (FUNC-SPEC
// §8.7 T5, T6).
func (c *Client) CreateTopics(ctx context.Context, specs []kafka.TopicSpec, validateOnly bool) ([]kafka.TopicCreateResult, error) {
	out := make([]kafka.TopicCreateResult, len(specs))
	for i, spec := range specs {
		configs := toKadmConfigPointers(spec.Configs)

		if validateOnly {
			resp, err := c.kadm.ValidateCreateTopics(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
			if err != nil {
				return nil, wrapErr("topic", err)
			}
			r, rerr := resp.On(spec.Name, nil)
			out[i] = toTopicCreateResult(spec.Name, r, rerr)
			continue
		}

		r, err := c.kadm.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
		out[i] = toTopicCreateResult(spec.Name, r, err)
	}
	return out, nil
}

// toTopicCreateResult converts one kadm create-topic response into the
// port's shape; rerr (from CreateTopicResponses.On, or CreateTopic's own
// returned error) and r.Err both surface as this spec's own error, never a
// call-wide failure.
func toTopicCreateResult(name string, r kadm.CreateTopicResponse, rerr error) kafka.TopicCreateResult {
	if rerr != nil {
		return kafka.TopicCreateResult{Name: name, Err: wrapErr("topic", rerr)}
	}
	if r.Err != nil {
		return kafka.TopicCreateResult{Name: name, Err: wrapErr("topic", r.Err)}
	}
	configsSlice := make([]kadm.Config, 0, len(r.Configs))
	for _, cfg := range r.Configs {
		configsSlice = append(configsSlice, cfg)
	}
	return kafka.TopicCreateResult{
		Name: r.Topic, Partitions: r.NumPartitions, ReplicationFactor: r.ReplicationFactor,
		Configs: toConfigEntries(configsSlice),
	}
}

// toKadmConfigPointers converts a plain string map into the *string map
// kadm's config-bearing calls take, so a config can be distinguished from
// "unset" (kadm's own convention).
func toKadmConfigPointers(configs map[string]string) map[string]*string {
	if len(configs) == 0 {
		return nil
	}
	out := make(map[string]*string, len(configs))
	for k, v := range configs {
		v := v
		out[k] = &v
	}
	return out
}

// IncrementalAlterTopicConfigs applies changes to topic's configuration via
// kadm.AlterTopicConfigs (FUNC-SPEC §8.7 T9): a non-nil Value sets that key,
// a nil Value resets it to its default. Unknown topic → NotFound.
func (c *Client) IncrementalAlterTopicConfigs(ctx context.Context, topic string, changes []kafka.ConfigChange) error {
	kadmChanges := make([]kadm.AlterConfig, len(changes))
	for i, ch := range changes {
		if ch.Value != nil {
			kadmChanges[i] = kadm.AlterConfig{Op: kadm.SetConfig, Name: ch.Name, Value: ch.Value}
		} else {
			kadmChanges[i] = kadm.AlterConfig{Op: kadm.DeleteConfig, Name: ch.Name}
		}
	}

	resp, err := c.kadm.AlterTopicConfigs(ctx, kadmChanges, topic)
	if err != nil {
		return wrapErr("topic", err)
	}
	r, err := resp.On(topic, nil)
	if err != nil {
		return wrapErr("topic", err)
	}
	if r.Err != nil {
		return wrapErr("topic", r.Err)
	}
	return nil
}

// CreatePartitions sets topic's partition count to total via
// kadm.UpdatePartitions (an absolute target, unlike kadm's own
// CreatePartitions, which takes a delta) (FUNC-SPEC §8.7 T10). Unknown
// topic → NotFound.
func (c *Client) CreatePartitions(ctx context.Context, topic string, total int32) error {
	resp, err := c.kadm.UpdatePartitions(ctx, int(total), topic)
	if err != nil {
		return wrapErr("topic", err)
	}
	r, err := resp.On(topic, nil)
	if err != nil {
		return wrapErr("topic", err)
	}
	if r.Err != nil {
		return wrapErr("topic", r.Err)
	}
	return nil
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
