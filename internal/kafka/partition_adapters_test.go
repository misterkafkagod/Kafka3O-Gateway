package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type placementRequesterFake struct {
	request  *kmsg.MetadataRequest
	response *kmsg.MetadataResponse
	err      error
}

func (f *placementRequesterFake) Request(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
	f.request = request.(*kmsg.MetadataRequest)
	return f.response, f.err
}

type partitionCountAdminFake struct {
	calls     []bool
	count     int
	topic     string
	responses kadm.CreatePartitionsResponses
	err       error
}

func (f *partitionCountAdminFake) ValidateUpdatePartitions(_ context.Context, count int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	f.calls = append(f.calls, true)
	f.count, f.topic = count, topics[0]
	return f.responses, f.err
}

func (f *partitionCountAdminFake) UpdatePartitions(_ context.Context, count int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	f.calls = append(f.calls, false)
	f.count, f.topic = count, topics[0]
	return f.responses, f.err
}

type reassignmentsAdminFake struct {
	listRequest  kadm.TopicsSet
	alterRequest kadm.AlterPartitionAssignmentsReq
	listResult   kadm.ListPartitionReassignmentsResponses
	alterResult  kadm.AlterPartitionAssignmentsResponses
	err          error
}

func (f *reassignmentsAdminFake) ListPartitionReassignments(_ context.Context, request kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error) {
	f.listRequest = request
	return f.listResult, f.err
}

func (f *reassignmentsAdminFake) AlterPartitionAssignments(_ context.Context, request kadm.AlterPartitionAssignmentsReq) (kadm.AlterPartitionAssignmentsResponses, error) {
	f.alterRequest = request
	return f.alterResult, f.err
}

func TestDescribePlacementRequestsLiveMetadataWithoutCreatingTopics(t *testing.T) {
	topic := "orders"
	requester := &placementRequesterFake{response: &kmsg.MetadataResponse{
		Brokers: []kmsg.MetadataResponseBroker{{NodeID: 2}, {NodeID: 1}},
		Topics: []kmsg.MetadataResponseTopic{{Topic: &topic, Partitions: []kmsg.MetadataResponseTopicPartition{
			{Partition: 0, Replicas: []int32{1, 2}},
		}}},
	}}
	got, err := NewDescribePlacement(requester).Execute(context.Background(), topic)
	if err != nil || !reflect.DeepEqual(got.Brokers, []int32{1, 2}) || !reflect.DeepEqual(got.Partitions[0], []int32{1, 2}) ||
		requester.request.AllowAutoTopicCreation || len(requester.request.Topics) != 1 || *requester.request.Topics[0].Topic != topic {
		t.Fatalf("error %v, placement %#v, request %#v", err, got, requester.request)
	}
}

func TestDescribePlacementSurfacesMetadataFailures(t *testing.T) {
	topic, other := "orders", "other"
	for _, test := range []struct {
		name     string
		response *kmsg.MetadataResponse
		err      error
		want     error
	}{
		{"unreachable", nil, errors.New("offline"), nil},
		{"missing", &kmsg.MetadataResponse{}, nil, partitions.ErrNotFound},
		{"wrong topic", &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{Topic: &other}}}, nil, nil},
		{"denied", &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{Topic: &topic, ErrorCode: kerr.TopicAuthorizationFailed.Code}}}, nil, partitions.ErrDenied},
		{"partition error", &kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{{Topic: &topic, Partitions: []kmsg.MetadataResponseTopicPartition{{ErrorCode: kerr.LeaderNotAvailable.Code}}}}}, nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewDescribePlacement(&placementRequesterFake{response: test.response, err: test.err}).Execute(context.Background(), topic)
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("error %v, want %v", err, test.want)
			}
		})
	}
}

func TestUpdateKafkaPartitionsUsesAbsoluteCountAndChecksResult(t *testing.T) {
	admin := &partitionCountAdminFake{responses: kadm.CreatePartitionsResponses{"orders": {Topic: "orders"}}}
	adapter := NewUpdateKafkaPartitions(admin)
	for _, validate := range []bool{true, false} {
		if err := adapter.Execute(context.Background(), "orders", 4, validate); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(admin.calls, []bool{true, false}) || admin.count != 4 || admin.topic != "orders" {
		t.Fatalf("unexpected calls %#v", admin)
	}
	admin.responses["orders"] = kadm.CreatePartitionsResponse{Err: kerr.InvalidPartitions}
	if err := adapter.Execute(context.Background(), "orders", 4, true); !errors.Is(err, partitions.ErrRejected) {
		t.Fatalf("error %v, want rejection", err)
	}
	admin.responses = kadm.CreatePartitionsResponses{}
	if err := adapter.Execute(context.Background(), "orders", 4, false); err == nil {
		t.Fatal("expected an error for an omitted response")
	}
}

func TestReassignmentAdaptersScopeRequestsAndNeverCancel(t *testing.T) {
	admin := &reassignmentsAdminFake{
		listResult: kadm.ListPartitionReassignmentsResponses{"orders": {
			1: {Topic: "orders", Partition: 1, Replicas: []int32{1, 2}, AddingReplicas: []int32{2}, RemovingReplicas: []int32{1}},
		}},
		alterResult: kadm.AlterPartitionAssignmentsResponses{"orders": {1: {Topic: "orders", Partition: 1}}},
	}
	list := NewListKafkaReassignments(admin)
	got, err := list.Execute(context.Background(), "orders", []int32{0, 1})
	if err != nil || len(got) != 1 || got[0].Partition != 1 || len(admin.listRequest["orders"]) != 2 {
		t.Fatalf("list error %v, result %#v, request %#v", err, got, admin.listRequest)
	}
	alter := NewAlterKafkaReassignment(admin)
	if err := alter.Execute(context.Background(), "orders", 1, nil); !errors.Is(err, partitions.ErrInvalidChange) || admin.alterRequest != nil {
		t.Fatalf("empty assignment must not cancel: error %v, request %#v", err, admin.alterRequest)
	}
	if err := alter.Execute(context.Background(), "orders", 1, []int32{2, 3}); err != nil ||
		!reflect.DeepEqual(admin.alterRequest["orders"][1], []int32{2, 3}) {
		t.Fatalf("alter error %v, request %#v", err, admin.alterRequest)
	}
	admin.alterResult = nil
	if err := alter.Execute(context.Background(), "orders", 1, []int32{2, 3}); err == nil {
		t.Fatal("expected an error for an omitted partition response")
	}
	admin.listResult = kadm.ListPartitionReassignmentsResponses{"other": {1: {Topic: "other", Partition: 1}}}
	if _, err := list.Execute(context.Background(), "orders", []int32{1}); err == nil {
		t.Fatal("expected an error for an unexpected partition")
	}
	admin.listResult = kadm.ListPartitionReassignmentsResponses{"orders": {1: {Topic: "orders", Partition: 1}}}
	got, err = list.Execute(context.Background(), "orders", []int32{1})
	if err != nil || len(got) != 1 || got[0].Replicas == nil || got[0].AddingReplicas == nil || got[0].RemovingReplicas == nil {
		t.Fatalf("reassignment arrays must be non-nil: result %#v, error %v", got, err)
	}
}
