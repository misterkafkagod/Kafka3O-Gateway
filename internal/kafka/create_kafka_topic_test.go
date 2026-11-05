package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeKafkaTopicCreator struct {
	partitions int32
	replicas   int16
	name       string
	err        error
}

func (f *fakeKafkaTopicCreator) CreateTopic(_ context.Context, partitions int32, replicas int16, _ map[string]*string, name string) (kadm.CreateTopicResponse, error) {
	f.partitions, f.replicas, f.name = partitions, replicas, name
	return kadm.CreateTopicResponse{}, f.err
}

func TestCreateKafkaTopic_MapsRequestAndErrors(t *testing.T) {
	request := topics.CreateTopicRequest{Name: "orders", Partitions: 3, ReplicationFactor: 2}
	creator := &fakeKafkaTopicCreator{}
	action := NewCreateKafkaTopic(creator)
	if err := action.Execute(context.Background(), request); err != nil || creator.partitions != 3 || creator.replicas != 2 || creator.name != "orders" {
		t.Fatalf("creator = %#v, error = %v", creator, err)
	}
	creator.err = kerr.TopicAlreadyExists
	if err := action.Execute(context.Background(), request); !errors.Is(err, topics.ErrTopicAlreadyExists) {
		t.Fatalf("duplicate error = %v", err)
	}
	want := errors.New("connection lost")
	creator.err = want
	if err := action.Execute(context.Background(), request); !errors.Is(err, want) {
		t.Fatalf("broker error = %v", err)
	}
}
