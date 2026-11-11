package kafka

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

const clusterMetadataTopic = "__cluster_metadata"

type readQuorum struct {
	client kmsg.Requestor
}

// NewReadQuorum adapts DescribeQuorum (the quorum-and-leader-metrics recipe) to the metrics.ReadQuorum port;
// kadm has no helper for it.
func NewReadQuorum(client kmsg.Requestor) metrics.ReadQuorum {
	return readQuorum{client: client}
}

func (r readQuorum) Execute(ctx context.Context) (metrics.Quorum, error) {
	request := kmsg.NewPtrDescribeQuorumRequest()
	topic := kmsg.NewDescribeQuorumRequestTopic()
	topic.Topic = clusterMetadataTopic
	topic.Partitions = append(topic.Partitions, kmsg.NewDescribeQuorumRequestTopicPartition())
	request.Topics = append(request.Topics, topic)
	response, err := request.RequestWith(ctx, r.client)
	if err != nil {
		return metrics.Quorum{}, fmt.Errorf("describe quorum: %w", err)
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return metrics.Quorum{}, fmt.Errorf("describe quorum: %w", err)
	}
	for _, described := range response.Topics {
		for _, partition := range described.Partitions {
			if described.Topic != clusterMetadataTopic || partition.Partition != 0 {
				continue
			}
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return metrics.Quorum{}, fmt.Errorf("describe quorum: %w", err)
			}
			quorum := metrics.Quorum{LeaderEpoch: partition.LeaderEpoch, HighWatermark: partition.HighWatermark, Replicas: []metrics.QuorumReplica{}}
			var leaderEnd *int64
			if partition.LeaderID >= 0 {
				leader := partition.LeaderID
				quorum.LeaderID = &leader
				for _, voter := range partition.CurrentVoters {
					if voter.ReplicaID == leader {
						end := voter.LogEndOffset
						leaderEnd = &end
					}
				}
			}
			for role, replicas := range map[string][]kmsg.DescribeQuorumResponseTopicPartitionReplicaState{"voter": partition.CurrentVoters, "observer": partition.Observers} {
				for _, replica := range replicas {
					entry := metrics.QuorumReplica{ID: replica.ReplicaID, Role: role, LogEndOffset: replica.LogEndOffset}
					if leaderEnd != nil {
						lag := max(*leaderEnd-replica.LogEndOffset, 0)
						entry.Lag = &lag
					}
					if replica.LastFetchTimestamp >= 0 {
						at := time.UnixMilli(replica.LastFetchTimestamp).UTC()
						entry.LastFetch = &at
					}
					if replica.LastCaughtUpTimestamp >= 0 {
						at := time.UnixMilli(replica.LastCaughtUpTimestamp).UTC()
						entry.LastCaughtUp = &at
					}
					quorum.Replicas = append(quorum.Replicas, entry)
				}
			}
			slices.SortFunc(quorum.Replicas, metrics.CompareQuorumReplicas)
			return quorum, nil
		}
	}
	return metrics.Quorum{}, fmt.Errorf("describe quorum: Kafka did not describe %s", clusterMetadataTopic)
}
