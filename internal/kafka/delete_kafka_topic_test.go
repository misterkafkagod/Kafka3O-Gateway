package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeKafkaTopicDeleter struct {
	name string
	err  error
}

func (f *fakeKafkaTopicDeleter) DeleteTopic(_ context.Context, name string) (kadm.DeleteTopicResponse, error) {
	f.name = name
	return kadm.DeleteTopicResponse{}, f.err
}

func TestDeleteKafkaTopic_MapsRequestAndErrors(t *testing.T) {
	adapter := &fakeKafkaTopicDeleter{}
	action := NewDeleteKafkaTopic(adapter)
	if err := action.Execute(context.Background(), "orders"); err != nil || adapter.name != "orders" {
		t.Fatalf("error = %v, name = %q", err, adapter.name)
	}
	for _, missing := range []error{kerr.UnknownTopicOrPartition, kerr.UnknownTopicID} {
		adapter.err = missing
		if err := action.Execute(context.Background(), "orders"); !errors.Is(err, topics.ErrTopicNotFound) {
			t.Fatalf("missing error = %v", err)
		}
	}
	want := errors.New("connection lost")
	adapter.err = want
	if err := action.Execute(context.Background(), "orders"); !errors.Is(err, want) {
		t.Fatalf("broker error = %v", err)
	}
}
