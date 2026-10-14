package api

import (
	"errors"
	"net/http"
	"testing"
)

func TestHealthReachable(t *testing.T) {
	var body healthResponse
	res := get(t, &fakeCluster{}, "/api/health", &body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if body.Gateway != "up" || !body.Cluster.Reachable || body.Cluster.Error != "" {
		t.Errorf("body = %+v, want gateway up, cluster reachable", body)
	}
}

func TestHealthPingFails(t *testing.T) {
	var body healthResponse
	res := get(t, &fakeCluster{pingErr: errors.New("dial tcp localhost:1: connection refused")}, "/api/health", &body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if body.Gateway != "up" || body.Cluster.Reachable {
		t.Errorf("body = %+v, want gateway up, cluster unreachable", body)
	}
	if body.Cluster.Error != "dial tcp localhost:1: connection refused" {
		t.Errorf("error = %q", body.Cluster.Error)
	}
}

func TestClusterThreeBrokersWithController(t *testing.T) {
	want := ClusterInfo{
		ClusterID:    "abc123",
		ControllerID: 2,
		Brokers: []Broker{
			{ID: 1, Host: "k1", Port: 9092, Rack: ptr("a")},
			{ID: 2, Host: "k2", Port: 9092},
			{ID: 3, Host: "k3", Port: 9093},
		},
	}
	var body ClusterInfo
	res := get(t, &fakeCluster{info: want}, "/api/cluster", &body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if body.ClusterID != "abc123" || body.ControllerID != 2 || len(body.Brokers) != 3 {
		t.Fatalf("body = %+v", body)
	}
	if b := body.Brokers[0]; b.ID != 1 || b.Host != "k1" || b.Port != 9092 || b.Rack == nil || *b.Rack != "a" {
		t.Errorf("broker[0] = %+v", b)
	}
	if b := body.Brokers[2]; b.ID != 3 || b.Port != 9093 || b.Rack != nil {
		t.Errorf("broker[2] = %+v", b)
	}
}

func TestClusterDescribeFails(t *testing.T) {
	var body errorBody
	res := get(t, &fakeCluster{describeErr: errors.New("no brokers reachable")}, "/api/cluster", &body)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if body.Error.Code != CodeClusterUnreachable || body.Error.Message != "no brokers reachable" {
		t.Errorf("error body = %+v", body.Error)
	}
}
