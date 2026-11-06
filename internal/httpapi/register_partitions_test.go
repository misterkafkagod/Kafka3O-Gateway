package httpapi

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type fakeIncreasePartitions struct {
	topic string
	count int32
	err   error
}

func (f *fakeIncreasePartitions) Execute(_ context.Context, topic string, count int32) error {
	f.topic, f.count = topic, count
	return f.err
}

type fakeListReassignments struct {
	topic string
	err   error
}

func (f *fakeListReassignments) Execute(_ context.Context, topic string) ([]partitions.Reassignment, error) {
	f.topic = topic
	return []partitions.Reassignment{{Partition: 0, Replicas: []int32{1, 2}, AddingReplicas: []int32{2}, RemovingReplicas: []int32{1}}}, f.err
}

type fakeReassignPartition struct {
	topic     string
	partition int32
	replicas  []int32
	err       error
}

func (f *fakeReassignPartition) Execute(_ context.Context, topic string, partition int32, replicas []int32) error {
	f.topic, f.partition, f.replicas = topic, partition, replicas
	return f.err
}

func TestPartitionEndpointsRouteRequests(t *testing.T) {
	_, api := humatest.New(t)
	increase, list, alter := &fakeIncreasePartitions{}, &fakeListReassignments{}, &fakeReassignPartition{}
	RegisterIncreasePartitions(api, increase)
	RegisterListReassignments(api, list)
	RegisterReassignPartition(api, alter)
	response := api.Post("/api/v1/topics/orders/partitions", map[string]any{"count": 4})
	if response.Code != http.StatusAccepted || increase.topic != "orders" || increase.count != 4 ||
		!strings.Contains(response.Body.String(), `"status":"accepted"`) {
		t.Fatalf("increase: status %d, action %#v, body %s", response.Code, increase, response.Body.String())
	}
	response = api.Get("/api/v1/topics/orders/reassignments")
	if response.Code != http.StatusOK || list.topic != "orders" || !strings.Contains(response.Body.String(), `"addingReplicas":[2]`) {
		t.Fatalf("list: status %d, action %#v, body %s", response.Code, list, response.Body.String())
	}
	response = api.Post("/api/v1/topics/orders/partitions/1/replicas", map[string]any{"replicas": []int32{2, 3}})
	if response.Code != http.StatusAccepted || alter.topic != "orders" || alter.partition != 1 || !reflect.DeepEqual(alter.replicas, []int32{2, 3}) {
		t.Fatalf("alter: status %d, action %#v, body %s", response.Code, alter, response.Body.String())
	}
}

func TestPartitionErrorsAreSafeAndExplicit(t *testing.T) {
	_, api := humatest.New(t)
	alter := &fakeReassignPartition{}
	RegisterReassignPartition(api, alter)
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{
		{partitions.ErrInvalidChange, http.StatusBadRequest, "invalid_partition_change"},
		{partitions.ErrNotFound, http.StatusNotFound, "partition_not_found"},
		{partitions.ErrDenied, http.StatusForbidden, "partition_denied"},
		{partitions.ErrRejected, http.StatusUnprocessableEntity, "partition_rejected"},
		{partitions.ErrInProgress, http.StatusConflict, "reassignment_in_progress"},
		{partitions.ErrWriteUnknown, http.StatusServiceUnavailable, "partition_write_unknown"},
		{errors.New("private upstream detail"), http.StatusServiceUnavailable, "cluster_unreachable"},
	} {
		alter.err = test.err
		response := api.Post("/api/v1/topics/orders/partitions/1/replicas", map[string]any{"replicas": []int32{2, 3}})
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.code) ||
			strings.Contains(response.Body.String(), "private upstream detail") {
			t.Errorf("error %v: status %d, body %s", test.err, response.Code, response.Body.String())
		}
	}
}

func TestReassignmentEndpointRejectsNullOrEmptyReplicas(t *testing.T) {
	_, api := humatest.New(t)
	alter := &fakeReassignPartition{}
	RegisterReassignPartition(api, alter)
	for _, replicas := range []any{nil, []int32{}} {
		response := api.Post("/api/v1/topics/orders/partitions/1/replicas", map[string]any{"replicas": replicas})
		if response.Code < 400 || alter.topic != "" {
			t.Fatalf("replicas %v: status %d, action %#v", replicas, response.Code, alter)
		}
	}
}
