package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fakeQuorumClient struct {
	response *kmsg.DescribeQuorumResponse
	err      error
	request  *kmsg.DescribeQuorumRequest
}

func (f *fakeQuorumClient) Request(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
	f.request = request.(*kmsg.DescribeQuorumRequest)
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

func replica(id int32, end, fetch int64) kmsg.DescribeQuorumResponseTopicPartitionReplicaState {
	state := kmsg.NewDescribeQuorumResponseTopicPartitionReplicaState()
	state.ReplicaID, state.LogEndOffset, state.LastFetchTimestamp, state.LastCaughtUpTimestamp = id, end, fetch, fetch
	return state
}

func quorumResponse(leader int32, voters, observers []kmsg.DescribeQuorumResponseTopicPartitionReplicaState) *kmsg.DescribeQuorumResponse {
	partition := kmsg.NewDescribeQuorumResponseTopicPartition()
	partition.LeaderID, partition.LeaderEpoch, partition.HighWatermark = leader, 3, 894587
	partition.CurrentVoters, partition.Observers = voters, observers
	topic := kmsg.NewDescribeQuorumResponseTopic()
	topic.Topic = "__cluster_metadata"
	topic.Partitions = append(topic.Partitions, partition)
	response := kmsg.NewPtrDescribeQuorumResponse()
	response.Topics = append(response.Topics, topic)
	return response
}

func TestReadQuorumAsOnKafkaA(t *testing.T) {
	client := &fakeQuorumClient{response: quorumResponse(1, []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{replica(1, 894587, 1794370626491)}, nil)}
	quorum, err := NewReadQuorum(client).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if client.request.Topics[0].Topic != "__cluster_metadata" || client.request.Topics[0].Partitions[0].Partition != 0 {
		t.Fatalf("request = %+v", client.request.Topics)
	}
	if *quorum.LeaderID != 1 || quorum.LeaderEpoch != 3 || quorum.HighWatermark != 894587 || len(quorum.Replicas) != 1 ||
		*quorum.Replicas[0].Lag != 0 || quorum.Replicas[0].Role != "voter" || quorum.Replicas[0].LastFetch.UnixMilli() != 1794370626491 {
		t.Fatalf("quorum = %+v %+v", quorum, quorum.Replicas)
	}
}

func TestReadQuorumLagAndObservers(t *testing.T) {
	client := &fakeQuorumClient{response: quorumResponse(2,
		[]kmsg.DescribeQuorumResponseTopicPartitionReplicaState{replica(3, 900, 10), replica(2, 1000, 10), replica(1, 1000, -1)},
		[]kmsg.DescribeQuorumResponseTopicPartitionReplicaState{replica(4, 950, 10)})}
	quorum, err := NewReadQuorum(client).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[int32]int64{}
	for _, r := range quorum.Replicas {
		got[r.ID] = *r.Lag
	}
	if got[1] != 0 || got[2] != 0 || got[3] != 100 || got[4] != 50 || quorum.Replicas[3].Role != "observer" || quorum.Replicas[0].ID != 1 {
		t.Fatalf("replicas = %+v", quorum.Replicas)
	}
	if quorum.Replicas[0].LastFetch != nil || quorum.Replicas[0].LastCaughtUp != nil {
		t.Fatal("-1 timestamps must be unknown")
	}
}

func TestReadQuorumWithoutALeaderHasNoLag(t *testing.T) {
	client := &fakeQuorumClient{response: quorumResponse(-1, []kmsg.DescribeQuorumResponseTopicPartitionReplicaState{replica(1, 5, 10)}, nil)}
	quorum, err := NewReadQuorum(client).Execute(context.Background())
	if err != nil || quorum.LeaderID != nil || quorum.Replicas[0].Lag != nil {
		t.Fatalf("%+v %v", quorum, err)
	}
}

func TestReadQuorumErrors(t *testing.T) {
	topLevel := quorumResponse(1, nil, nil)
	topLevel.ErrorCode = kerr.ClusterAuthorizationFailed.Code
	partitionLevel := quorumResponse(1, nil, nil)
	partitionLevel.Topics[0].Partitions[0].ErrorCode = kerr.NotLeaderForPartition.Code
	missing := kmsg.NewPtrDescribeQuorumResponse()
	for name, client := range map[string]*fakeQuorumClient{
		"request":   {err: errors.New("connection refused")},
		"top level": {response: topLevel},
		"partition": {response: partitionLevel},
		"missing":   {response: missing},
	} {
		if _, err := NewReadQuorum(client).Execute(context.Background()); err == nil || !strings.HasPrefix(err.Error(), "describe quorum: ") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
