package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestListTopicMetadata_MapsTopicsAndRequestsAgain(t *testing.T) {
	orders := "orders"
	internal := "__consumer_offsets"
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{
		Topics: []kmsg.MetadataResponseTopic{
			{Topic: &orders, Partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, Replicas: []int32{1, 2}},
				{Partition: 1, Replicas: []int32{1, 2, 3}},
			}},
			{Topic: &internal, IsInternal: true, Partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, Replicas: []int32{1}},
			}},
		},
	}}
	action := NewListTopicMetadata(requester)

	got, err := action.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []topics.TopicMetadata{
		{Name: orders, Partitions: 2, ReplicationFactor: 3},
		{Name: internal, Partitions: 1, ReplicationFactor: 1, Internal: true},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("topic metadata mismatch (-want +got):\n%s", diff)
	}
	if requester.requests[0].Topics != nil {
		t.Errorf("requested topics = %#v, want all topics", requester.requests[0].Topics)
	}

	offline := errors.New("broker unavailable")
	requester.err = offline
	if _, err := action.Execute(context.Background()); !errors.Is(err, offline) {
		t.Errorf("second request error = %v, want %v", err, offline)
	}
	if len(requester.requests) != 2 {
		t.Errorf("request count = %d, want 2", len(requester.requests))
	}
}

func TestListTopicMetadata_EmptyResult(t *testing.T) {
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{}}
	got, err := NewListTopicMetadata(requester).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("topics = %#v, want a non-nil empty slice", got)
	}
}

func TestListTopicMetadata_PropagatesTopicAuthorizationError(t *testing.T) {
	name := "private"
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{
		Topics: []kmsg.MetadataResponseTopic{{Topic: &name, ErrorCode: kerr.TopicAuthorizationFailed.Code}},
	}}
	got, err := NewListTopicMetadata(requester).Execute(context.Background())
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Errorf("error = %v, want %v", err, kerr.TopicAuthorizationFailed)
	}
	if got != nil {
		t.Errorf("topics = %#v, want nil on error", got)
	}
}

func TestListTopicMetadata_RejectsMissingTopicName(t *testing.T) {
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{
		Topics: []kmsg.MetadataResponseTopic{{}},
	}}
	if _, err := NewListTopicMetadata(requester).Execute(context.Background()); err == nil {
		t.Error("expected an error for metadata with no topic name")
	}
}
