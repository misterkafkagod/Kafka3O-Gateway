package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func TestDescribeCluster_RequestsLiveMetadataAfterSuccess(t *testing.T) {
	clusterID := "cluster-1"
	rack := "rack-a"
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{
		ClusterID:    &clusterID,
		ControllerID: 1,
		Brokers:      []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "broker", Port: 9092, Rack: &rack}},
	}}
	action := NewDescribeCluster(requester)

	got, err := action.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := cluster.Cluster{ID: clusterID, ControllerID: 1, Brokers: []cluster.Broker{{ID: 1, Host: "broker", Port: 9092, Rack: &rack}}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("cluster mismatch (-want +got):\n%s", diff)
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

func TestDescribeCluster_ReturnsErrorWhenKafkaIsUnreachable(t *testing.T) {
	client, err := NewClient(config.KafkaConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err = NewDescribeCluster(client).Execute(ctx)

	if err == nil {
		t.Fatal("expected an error for an unreachable broker")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("returned after %v, want within 2s", elapsed)
	}
}
