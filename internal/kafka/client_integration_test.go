//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka/kafkatest"
)

func newClient(t *testing.T, brokers []string) *Client {
	t.Helper()
	c, err := New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestDescribeCluster(t *testing.T) {
	c := newClient(t, kafkatest.Start(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	info, err := c.DescribeCluster(ctx)
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	if info.ClusterID == "" {
		t.Error("ClusterID is empty")
	}
	if len(info.Brokers) != 1 {
		t.Fatalf("got %d brokers, want 1: %+v", len(info.Brokers), info.Brokers)
	}
	if b := info.Brokers[0]; b.ID != 1 || b.Host == "" || b.Port == 0 {
		t.Errorf("broker = %+v", b)
	}
	if info.ControllerID < 0 {
		t.Errorf("ControllerID = %d, want a node id", info.ControllerID)
	}
}

func TestPingUnreachable(t *testing.T) {
	c := newClient(t, []string{"localhost:1"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := c.Ping(ctx); err == nil {
		t.Fatal("Ping succeeded against localhost:1")
	}
	if d := time.Since(start); d > 3500*time.Millisecond {
		t.Errorf("Ping took %v, want at most ~3s", d)
	}
}
