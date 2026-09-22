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

// ListGroups returns every seeded group's state, protocol type, and member
// count, optionally filtered to the given states — an empty states filters
// nothing (FUNC-SPEC §8.7 G1).
func (f *Fake) ListGroups(ctx context.Context, states ...string) ([]kafka.GroupSummary, error) {
	return invoke(f, ctx, "ListGroups", false, func() ([]kafka.GroupSummary, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		var filter map[string]bool
		if len(states) > 0 {
			filter = make(map[string]bool, len(states))
			for _, s := range states {
				filter[s] = true
			}
		}

		out := make([]kafka.GroupSummary, 0, len(f.model.groups))
		for _, g := range f.sortedGroups() {
			if filter != nil && !filter[g.state] {
				continue
			}
			out = append(out, kafka.GroupSummary{
				ID: g.id, State: g.state, ProtocolType: g.protocolType, MemberCount: len(g.members),
			})
		}
		return out, nil
	})
}

// DescribeGroups returns full detail for groupIDs, or every seeded group
// when groupIDs is empty (FUNC-SPEC §8.7 G2, G3). Given explicit ids, any
// missing one fails the whole call as NotFound; given none, a missing group
// simply can't occur — every seeded group is described.
func (f *Fake) DescribeGroups(ctx context.Context, groupIDs ...string) ([]kafka.Group, error) {
	return invoke(f, ctx, "DescribeGroups", false, func() ([]kafka.Group, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		if len(groupIDs) > 0 {
			out := make([]kafka.Group, len(groupIDs))
			for i, id := range groupIDs {
				g := f.model.groups[id]
				if g == nil {
					return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "group"}
				}
				out[i] = toDomainGroup(g)
			}
			return out, nil
		}

		out := make([]kafka.Group, 0, len(f.model.groups))
		for _, g := range f.sortedGroups() {
			out = append(out, toDomainGroup(g))
		}
		return out, nil
	})
}

// toDomainGroup converts a fakeGroup into the port's Group shape. Callers
// must hold f.mu.
func toDomainGroup(g *fakeGroup) kafka.Group {
	return kafka.Group{
		ID: g.id, State: g.state, ProtocolType: g.protocolType, CoordinatorID: g.coordinatorID,
		Members: append([]kafka.GroupMember(nil), g.members...),
	}
}

// FetchGroupOffsets returns groupID's committed offset per topic partition
// (FUNC-SPEC §8.7 G2 offsets[], G3 lag). Unknown group → NotFound.
func (f *Fake) FetchGroupOffsets(ctx context.Context, groupID string) (map[kafka.TopicPartition]int64, error) {
	return invoke(f, ctx, "FetchGroupOffsets", false, func() (map[kafka.TopicPartition]int64, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		g := f.model.groups[groupID]
		if g == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "group"}
		}
		out := make(map[kafka.TopicPartition]int64, len(g.offsets))
		for tp, offset := range g.offsets {
			out[tp] = offset
		}
		return out, nil
	})
}

// CreateTopics creates every spec — or, when validateOnly, checks each
// without creating anything (FUNC-SPEC §8.7 T5, T6). An already-existing
// name reports that spec's own AlreadyExists; the rest of the batch is
// unaffected.
func (f *Fake) CreateTopics(ctx context.Context, specs []kafka.TopicSpec, validateOnly bool) ([]kafka.TopicCreateResult, error) {
	return invoke(f, ctx, "CreateTopics", !validateOnly, func() ([]kafka.TopicCreateResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		out := make([]kafka.TopicCreateResult, len(specs))
		for i, spec := range specs {
			if f.model.topics[spec.Name] != nil {
				out[i] = kafka.TopicCreateResult{Name: spec.Name, Err: &kafka.Error{Kind: kafka.KindAlreadyExists, Resource: "topic"}}
				continue
			}

			configs := make([]kafka.ConfigEntry, 0, len(spec.Configs))
			for name, value := range spec.Configs {
				configs = append(configs, kafka.ConfigEntry{Name: name, Value: value, Source: kafka.SourceDynamic})
			}
			out[i] = kafka.TopicCreateResult{
				Name: spec.Name, Partitions: spec.Partitions, ReplicationFactor: spec.ReplicationFactor, Configs: configs,
			}
			if validateOnly {
				continue
			}

			if f.model.topics == nil {
				f.model.topics = map[string]*fakeTopic{}
			}
			f.model.topics[spec.Name] = &fakeTopic{
				name: spec.Name, replicationFactor: int(spec.ReplicationFactor),
				partitions: make([]fakePartition, spec.Partitions),
				configs:    configs,
			}
		}
		return out, nil
	})
}

// IncrementalAlterTopicConfigs applies changes to topic's configuration
// (FUNC-SPEC §8.7 T9): a non-nil Value sets that key to Dynamic source, a
// nil Value resets an already-set key to Default source (the fake models
// no broker-level default catalog, so it keeps whatever value the key last
// had — only Source changes). Unknown topic → NotFound.
func (f *Fake) IncrementalAlterTopicConfigs(ctx context.Context, topic string, changes []kafka.ConfigChange) error {
	_, err := invoke(f, ctx, "IncrementalAlterTopicConfigs", true, func() (struct{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		t := f.model.topics[topic]
		if t == nil {
			return struct{}{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		for _, ch := range changes {
			idx := -1
			for i, c := range t.configs {
				if c.Name == ch.Name {
					idx = i
					break
				}
			}
			if ch.Value != nil {
				entry := kafka.ConfigEntry{Name: ch.Name, Value: *ch.Value, Source: kafka.SourceDynamic}
				if idx >= 0 {
					t.configs[idx] = entry
				} else {
					t.configs = append(t.configs, entry)
				}
				continue
			}
			if idx >= 0 {
				t.configs[idx].Source = kafka.SourceDefault
			}
		}
		return struct{}{}, nil
	})
	return err
}

// CreatePartitions sets topic's partition count to the absolute total
// (FUNC-SPEC §8.7 T10). Unknown topic → NotFound; total not greater than
// the current count → a broker-side error (the fake's own safety net —
// the service layer already checks this as PARTITION_MISMATCH before
// calling the port at all).
func (f *Fake) CreatePartitions(ctx context.Context, topic string, total int32) error {
	_, err := invoke(f, ctx, "CreatePartitions", true, func() (struct{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		t := f.model.topics[topic]
		if t == nil {
			return struct{}{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}
		if int(total) <= len(t.partitions) {
			return struct{}{}, &kafka.Error{Kind: kafka.KindBroker, Resource: "topic"}
		}
		t.partitions = append(t.partitions, make([]fakePartition, int(total)-len(t.partitions))...)
		return struct{}{}, nil
	})
	return err
}

// DeleteTopics deletes every named topic (FUNC-SPEC §8.7 T7, T8). A missing
// topic reports NotFound on its own result; it never fails the rest of the
// batch.
func (f *Fake) DeleteTopics(ctx context.Context, topics []string) ([]kafka.TopicDeleteResult, error) {
	return invoke(f, ctx, "DeleteTopics", true, func() ([]kafka.TopicDeleteResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		out := make([]kafka.TopicDeleteResult, len(topics))
		for i, name := range topics {
			if f.model.topics[name] == nil {
				out[i] = kafka.TopicDeleteResult{Name: name, Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}}
				continue
			}
			delete(f.model.topics, name)
			out[i] = kafka.TopicDeleteResult{Name: name}
		}
		return out, nil
	})
}

// DeleteRecords truncates topic's partitions to truncateTo (FUNC-SPEC §8.7
// T11, T12): each named partition's begin offset advances to truncateTo,
// dropping any records now below it, keeping the beginOffset +
// len(records) == end invariant intact (mirrors SeedCompactAway). A
// truncateTo beyond a partition's current end offset → a broker-side error
// (the fake's own safety net — the service layer already checks this while
// building the plan).
func (f *Fake) DeleteRecords(ctx context.Context, topic string, truncateTo map[int32]int64) ([]kafka.PartitionLowWatermark, error) {
	return invoke(f, ctx, "DeleteRecords", true, func() ([]kafka.PartitionLowWatermark, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}

		for partition, at := range truncateTo {
			if int(partition) < 0 || int(partition) >= len(t.partitions) {
				return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "partition"}
			}
			p := &t.partitions[partition]
			end := p.beginOffset + int64(len(p.records))
			if at > end {
				return nil, &kafka.Error{Kind: kafka.KindBroker, Resource: "topic"}
			}
		}

		out := make([]kafka.PartitionLowWatermark, 0, len(truncateTo))
		partitions := make([]int32, 0, len(truncateTo))
		for partition := range truncateTo {
			partitions = append(partitions, partition)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

		for _, partition := range partitions {
			at := truncateTo[partition]
			p := &t.partitions[partition]
			kept := p.records[:0:0]
			for _, r := range p.records {
				if r.Offset >= at {
					kept = append(kept, r)
				}
			}
			p.records = kept
			p.beginOffset = at
			out = append(out, kafka.PartitionLowWatermark{Partition: partition, LowWatermark: at})
		}
		return out, nil
	})
}

// CommitGroupOffsets commits offsets for group (FUNC-SPEC §8.7 G4), creating
// it when it does not yet exist. A group with active members → GroupActive.
func (f *Fake) CommitGroupOffsets(ctx context.Context, group string, offsets map[kafka.TopicPartition]int64) error {
	_, err := invoke(f, ctx, "CommitGroupOffsets", true, func() (struct{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		g := f.model.groups[group]
		if g != nil && len(g.members) > 0 {
			return struct{}{}, &kafka.Error{Kind: kafka.KindGroupActive, Resource: "group"}
		}
		if g == nil {
			g = &fakeGroup{id: group, state: "Empty"}
			if f.model.groups == nil {
				f.model.groups = map[string]*fakeGroup{}
			}
			f.model.groups[group] = g
		}
		if g.offsets == nil {
			g.offsets = map[kafka.TopicPartition]int64{}
		}
		for tp, at := range offsets {
			g.offsets[tp] = at
		}
		return struct{}{}, nil
	})
	return err
}

// DeleteGroups deletes every named group (FUNC-SPEC §8.7 G5). A missing or
// active group reports its own error on its own result; it never fails the
// rest of the batch.
func (f *Fake) DeleteGroups(ctx context.Context, groups []string) ([]kafka.GroupDeleteResult, error) {
	return invoke(f, ctx, "DeleteGroups", true, func() ([]kafka.GroupDeleteResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		out := make([]kafka.GroupDeleteResult, len(groups))
		for i, id := range groups {
			g := f.model.groups[id]
			if g == nil {
				out[i] = kafka.GroupDeleteResult{ID: id, Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "group"}}
				continue
			}
			if len(g.members) > 0 {
				out[i] = kafka.GroupDeleteResult{ID: id, Err: &kafka.Error{Kind: kafka.KindGroupActive, Resource: "group"}}
				continue
			}
			delete(f.model.groups, id)
			out[i] = kafka.GroupDeleteResult{ID: id}
		}
		return out, nil
	})
}

// LeaveGroup evicts members from group by their member id (FUNC-SPEC §8.7
// G6). A member not currently part of the group reports its own error on
// its own result; it never fails the rest of the batch.
func (f *Fake) LeaveGroup(ctx context.Context, group string, members []string) ([]kafka.LeaveGroupResult, error) {
	return invoke(f, ctx, "LeaveGroup", true, func() ([]kafka.LeaveGroupResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		g := f.model.groups[group]
		if g == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "group"}
		}

		out := make([]kafka.LeaveGroupResult, len(members))
		for i, id := range members {
			idx := -1
			for j, m := range g.members {
				if m.MemberID == id {
					idx = j
					break
				}
			}
			if idx < 0 {
				out[i] = kafka.LeaveGroupResult{MemberID: id, Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "member"}}
				continue
			}
			g.members = append(g.members[:idx], g.members[idx+1:]...)
			out[i] = kafka.LeaveGroupResult{MemberID: id}
		}
		return out, nil
	})
}

// DescribeQuorum returns the seeded KRaft quorum status (FUNC-SPEC §8.7 C6).
// To simulate a cluster still on ZooKeeper, use
// FailNext/FailAlways("DescribeQuorum", kafka.KindUnsupported).
func (f *Fake) DescribeQuorum(ctx context.Context) (kafka.QuorumStatus, error) {
	return invoke(f, ctx, "DescribeQuorum", false, func() (kafka.QuorumStatus, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		q := f.model.quorum
		return kafka.QuorumStatus{
			LeaderID: q.leaderID, Epoch: q.epoch,
			Voters:    append([]kafka.QuorumReplicaState(nil), q.voters...),
			Observers: append([]kafka.QuorumReplicaState(nil), q.observers...),
		}, nil
	})
}

// ListReassignments returns every partition, cluster-wide, that currently
// has a non-nil addingReplicas or removingReplicas (FUNC-SPEC §8.7 C7).
func (f *Fake) ListReassignments(ctx context.Context) ([]kafka.PartitionReassignment, error) {
	return invoke(f, ctx, "ListReassignments", false, func() ([]kafka.PartitionReassignment, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		var out []kafka.PartitionReassignment
		for name, t := range f.model.topics {
			for i, p := range t.partitions {
				if len(p.addingReplicas) == 0 && len(p.removingReplicas) == 0 {
					continue
				}
				out = append(out, kafka.PartitionReassignment{
					Topic: name, Partition: int32(i),
					Replicas:         append([]int32(nil), p.replicas...),
					AddingReplicas:   append([]int32(nil), p.addingReplicas...),
					RemovingReplicas: append([]int32(nil), p.removingReplicas...),
				})
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Topic != out[j].Topic {
				return out[i].Topic < out[j].Topic
			}
			return out[i].Partition < out[j].Partition
		})
		return out, nil
	})
}

// AlterPartitionAssignments moves partitions' replicas, applying the change
// immediately (the fake has no background ISR-catch-up process), or clears
// a partition's pending reassignment given a nil replica set (FUNC-SPEC
// §8.7 C9 reassign, cancel). Each partition's own error surfaces on its own
// result; one failure never fails the rest of the batch.
func (f *Fake) AlterPartitionAssignments(ctx context.Context, moves map[kafka.TopicPartition][]int32) ([]kafka.ReassignResult, error) {
	return invoke(f, ctx, "AlterPartitionAssignments", true, func() ([]kafka.ReassignResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		tps := make([]kafka.TopicPartition, 0, len(moves))
		for tp := range moves {
			tps = append(tps, tp)
		}
		sort.Slice(tps, func(i, j int) bool {
			if tps[i].Topic != tps[j].Topic {
				return tps[i].Topic < tps[j].Topic
			}
			return tps[i].Partition < tps[j].Partition
		})

		out := make([]kafka.ReassignResult, len(tps))
		for i, tp := range tps {
			t := f.model.topics[tp.Topic]
			if t == nil || int(tp.Partition) < 0 || int(tp.Partition) >= len(t.partitions) {
				out[i] = kafka.ReassignResult{Topic: tp.Topic, Partition: tp.Partition, Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "partition"}}
				continue
			}
			p := &t.partitions[tp.Partition]
			newReplicas := moves[tp]
			if newReplicas == nil {
				p.addingReplicas = nil
				p.removingReplicas = nil
			} else {
				p.addingReplicas = replicasOnlyIn(newReplicas, p.replicas)
				p.removingReplicas = replicasOnlyIn(p.replicas, newReplicas)
				p.replicas = newReplicas
			}
			out[i] = kafka.ReassignResult{Topic: tp.Topic, Partition: tp.Partition}
		}
		return out, nil
	})
}

// replicasOnlyIn returns the elements of a not present in b.
func replicasOnlyIn(a, b []int32) []int32 {
	inB := make(map[int32]bool, len(b))
	for _, r := range b {
		inB[r] = true
	}
	var out []int32
	for _, r := range a {
		if !inB[r] {
			out = append(out, r)
		}
	}
	return out
}

// ElectLeaders sets each named partition's leader to its first replica —
// the fake models no ISR or broker-liveness state, so preferred and unclean
// election have the same observable effect here (FUNC-SPEC §8.7 C9 elect).
// Each partition's own error surfaces on its own result; one failure never
// fails the rest of the batch.
func (f *Fake) ElectLeaders(ctx context.Context, preferred bool, partitions []kafka.TopicPartition) ([]kafka.ElectLeaderResult, error) {
	return invoke(f, ctx, "ElectLeaders", true, func() ([]kafka.ElectLeaderResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		out := make([]kafka.ElectLeaderResult, len(partitions))
		for i, tp := range partitions {
			t := f.model.topics[tp.Topic]
			if t == nil || int(tp.Partition) < 0 || int(tp.Partition) >= len(t.partitions) {
				out[i] = kafka.ElectLeaderResult{Topic: tp.Topic, Partition: tp.Partition, Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "partition"}}
				continue
			}
			p := &t.partitions[tp.Partition]
			if len(p.replicas) > 0 {
				p.leader = p.replicas[0]
			}
			out[i] = kafka.ElectLeaderResult{Topic: tp.Topic, Partition: tp.Partition}
		}
		return out, nil
	})
}

// IncrementalAlterBrokerConfigs applies changes to brokerID's configuration
// (FUNC-SPEC §8.7 C5): a non-nil Value sets that key to Dynamic source, a
// nil Value resets an already-set key to Default source (the fake models no
// broker-level default catalog, the same simplification
// IncrementalAlterTopicConfigs makes). Unknown brokerID → NotFound.
func (f *Fake) IncrementalAlterBrokerConfigs(ctx context.Context, brokerID int32, changes []kafka.ConfigChange) error {
	_, err := invoke(f, ctx, "IncrementalAlterBrokerConfigs", true, func() (struct{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		if !f.hasBroker(brokerID) {
			return struct{}{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "broker"}
		}
		configs := f.model.brokerConfigs[brokerID]
		byName := make(map[string]int, len(configs))
		for i, c := range configs {
			byName[c.Name] = i
		}
		for _, ch := range changes {
			if i, ok := byName[ch.Name]; ok {
				if ch.Value != nil {
					configs[i].Value = *ch.Value
					configs[i].Source = kafka.SourceDynamic
				} else {
					configs[i].Source = kafka.SourceDefault
				}
				continue
			}
			if ch.Value != nil {
				configs = append(configs, kafka.ConfigEntry{Name: ch.Name, Value: *ch.Value, Source: kafka.SourceDynamic})
			}
		}
		f.model.brokerConfigs[brokerID] = configs
		return struct{}{}, nil
	})
	return err
}

// DescribeAllLogDirs aggregates every seeded per-partition, per-replica log
// directory into one entry per (broker, directory) (FUNC-SPEC §8.7 C8).
func (f *Fake) DescribeAllLogDirs(ctx context.Context) ([]kafka.BrokerLogDir, error) {
	return invoke(f, ctx, "DescribeAllLogDirs", false, func() ([]kafka.BrokerLogDir, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		type key struct {
			broker int32
			dir    string
		}
		agg := map[key]*kafka.BrokerLogDir{}
		for _, t := range f.model.topics {
			for _, p := range t.partitions {
				for brokerID, entry := range p.logDir {
					k := key{broker: brokerID, dir: entry.dir}
					d, ok := agg[k]
					if !ok {
						d = &kafka.BrokerLogDir{BrokerID: brokerID, LogDir: entry.dir}
						agg[k] = d
					}
					d.TotalBytes += entry.bytes
					d.PartitionCount++
				}
			}
		}
		out := make([]kafka.BrokerLogDir, 0, len(agg))
		for _, d := range agg {
			out = append(out, *d)
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].BrokerID != out[j].BrokerID {
				return out[i].BrokerID < out[j].BrokerID
			}
			return out[i].LogDir < out[j].LogDir
		})
		return out, nil
	})
}

// sortedGroups returns every seeded group, ordered by id for stable listing
// (FUNC-SPEC §9.7 Pagination "stable name ordering"). Callers must hold f.mu.
func (f *Fake) sortedGroups() []*fakeGroup {
	ids := make([]string, 0, len(f.model.groups))
	for id := range f.model.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*fakeGroup, len(ids))
	for i, id := range ids {
		out[i] = f.model.groups[id]
	}
	return out
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
