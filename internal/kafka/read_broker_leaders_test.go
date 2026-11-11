package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type fakeMetadataAdmin struct {
	metadata kadm.Metadata
	err      error
}

func (f fakeMetadataAdmin) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return f.metadata, f.err
}

func TestReadBrokerLeadersCountsPerBroker(t *testing.T) {
	metadata := kadm.Metadata{
		Brokers: kadm.BrokerDetails{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}, {NodeID: 4}},
		Topics: kadm.TopicDetails{
			"orders": {Topic: "orders", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Leader: 1, Replicas: []int32{1, 2}},
				1: {Partition: 1, Leader: 1, Replicas: []int32{2, 1}},
				2: {Partition: 2, Leader: 3, Replicas: []int32{3, 1}},
				3: {Partition: 3, Leader: -1, Replicas: []int32{2, 3}, Err: kerr.LeaderNotAvailable},
			}},
		},
	}
	got, err := NewReadBrokerLeaders(fakeMetadataAdmin{metadata: metadata}).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[int32][3]int{1: {2, 3, 1}, 2: {0, 3, 0}, 3: {1, 2, 0}, 4: {0, 0, 0}}
	if len(got) != 4 || got[0].Broker != 1 || got[3].Broker != 4 {
		t.Fatalf("brokers = %+v", got)
	}
	for _, broker := range got {
		if [3]int{broker.Leaders, broker.Replicas, broker.NonPreferredLeaders} != want[broker.Broker] {
			t.Errorf("broker %d = %+v, want %v", broker.Broker, broker, want[broker.Broker])
		}
	}
}

func TestReadBrokerLeadersFailure(t *testing.T) {
	if _, err := NewReadBrokerLeaders(fakeMetadataAdmin{err: errors.New("timeout")}).Execute(context.Background()); err == nil || err.Error() != "read metadata: timeout" {
		t.Fatalf("err = %v", err)
	}
}
