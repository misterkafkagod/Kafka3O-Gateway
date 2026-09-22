package franz

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// metadataQuorumTopic names the one topic-partition a DescribeQuorumRequest
// can ever target: KRaft replicates exactly one Raft log, the cluster
// metadata partition itself (KIP-595) — there is no per-topic notion of
// "quorum" the way there is for ordinary topics.
const metadataQuorumTopic = "__cluster_metadata"

// buildDescribeQuorumRequest builds the one DescribeQuorumRequest shape C6
// ever sends, factored out so a unit test can assert on its construction
// without a live broker (TECH-SPEC §1.1).
func buildDescribeQuorumRequest() *kmsg.DescribeQuorumRequest {
	req := kmsg.NewPtrDescribeQuorumRequest()
	topic := kmsg.NewDescribeQuorumRequestTopic()
	topic.Topic = metadataQuorumTopic
	partition := kmsg.NewDescribeQuorumRequestTopicPartition()
	partition.Partition = 0
	topic.Partitions = append(topic.Partitions, partition)
	req.Topics = append(req.Topics, topic)
	return req
}

// DescribeQuorum returns the KRaft quorum's current status via a raw
// kmsg.DescribeQuorumRequest (FUNC-SPEC §8.7 C6; TECH-SPEC §1.1: kadm has no
// wrapper for this KRaft-only API). A ZooKeeper-mode cluster's broker
// rejects the request with UNSUPPORTED_VERSION, which wrapErr already
// classifies as KindUnsupported (TECH-SPEC C3).
func (c *Client) DescribeQuorum(ctx context.Context) (kafka.QuorumStatus, error) {
	resp, err := buildDescribeQuorumRequest().RequestWith(ctx, c.kgo)
	if err != nil {
		return kafka.QuorumStatus{}, wrapErr("", err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return kafka.QuorumStatus{}, wrapErr("", err)
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return kafka.QuorumStatus{}, &kafka.Error{Kind: kafka.KindUnsupported}
	}
	p := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return kafka.QuorumStatus{}, wrapErr("", err)
	}

	now := time.Now()
	return kafka.QuorumStatus{
		LeaderID: p.LeaderID, Epoch: p.LeaderEpoch,
		Voters:    toQuorumReplicaStates(p.CurrentVoters, p.LeaderID, now),
		Observers: toQuorumReplicaStates(p.Observers, p.LeaderID, now),
	}, nil
}

// toQuorumReplicaStates converts kmsg's per-replica quorum state into the
// port's shape. LagMs is how long ago (from now) this replica last fully
// caught up to the leader's log — 0 for the leader's own entry, whose
// LastCaughtUpTimestamp is always -1 ("unknown for a voter" / n/a for the
// leader itself, per the field's own doc comment).
func toQuorumReplicaStates(states []kmsg.DescribeQuorumResponseTopicPartitionReplicaState, leaderID int32, now time.Time) []kafka.QuorumReplicaState {
	out := make([]kafka.QuorumReplicaState, len(states))
	for i, s := range states {
		var lag int64
		if s.ReplicaID != leaderID && s.LastCaughtUpTimestamp >= 0 {
			lag = now.UnixMilli() - s.LastCaughtUpTimestamp
			if lag < 0 {
				lag = 0
			}
		}
		out[i] = kafka.QuorumReplicaState{ID: s.ReplicaID, LogEndOffset: s.LogEndOffset, LagMs: lag}
	}
	return out
}
