package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/google/go-cmp/cmp"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
)

type fakeGetCluster struct {
	cluster cluster.Cluster
}

func (f fakeGetCluster) Execute(context.Context) (cluster.Cluster, error) {
	return f.cluster, nil
}

func TestRegisterGetCluster_ReturnsClusterJSON(t *testing.T) {
	rack := "eu-1a"
	_, api := humatest.New(t)
	RegisterGetCluster(api, fakeGetCluster{cluster: cluster.Cluster{
		ID:           "abc",
		ControllerID: 2,
		Brokers: []cluster.Broker{
			{ID: 1, Host: "kafka-1", Port: 9092, Rack: &rack},
			{ID: 2, Host: "kafka-2", Port: 9092},
		},
	}})

	resp := api.Get("/api/v1/cluster")

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	delete(got, "$schema")
	want := map[string]any{
		"clusterId":    "abc",
		"controllerId": float64(2),
		"brokers": []any{
			map[string]any{"id": float64(1), "host": "kafka-1", "port": float64(9092), "rack": "eu-1a"},
			map[string]any{"id": float64(2), "host": "kafka-2", "port": float64(9092)},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("body mismatch (-want +got):\n%s", diff)
	}
}
