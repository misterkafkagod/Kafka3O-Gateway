//go:build integration

package integration

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var externalTestBrokerUsed atomic.Bool

func UseExternalTestBroker(t *testing.T, address, disposable string) []string {
	t.Helper()
	if err := ValidateExternalTestBroker(address, disposable); err != nil {
		t.Fatal(err)
	}
	if !externalTestBrokerUsed.CompareAndSwap(false, true) {
		t.Fatal("external broker mode requires one integration test per process and a fresh broker per test")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		t.Fatalf("create external Kafka client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	admin := kadm.NewClient(client)
	cluster, err := admin.Metadata(ctx)
	if err != nil {
		t.Fatalf("inspect external Kafka cluster: %v", err)
	}
	if cluster.Cluster != kafkaClusterID || cluster.Controller != 1 || len(cluster.Brokers) != 1 || cluster.Brokers[0].NodeID != 1 {
		t.Fatal("external Kafka must be the disposable single-node integration fixture")
	}
	host, port, _ := net.SplitHostPort(address)
	broker := cluster.Brokers[0]
	if !net.ParseIP(broker.Host).Equal(net.ParseIP(host)) || strconv.Itoa(int(broker.Port)) != port {
		t.Fatal("external Kafka must advertise the configured loopback address and port")
	}
	for _, topic := range cluster.Topics {
		if topic.Err != nil || topic.Topic == "" {
			t.Fatalf("inspect external Kafka topic %q: %v", topic.Topic, topic.Err)
		}
		if !topic.IsInternal {
			t.Fatalf("external Kafka is not empty: topic %q exists; create a fresh broker per test", topic.Topic)
		}
	}
	groups, err := admin.ListGroups(ctx)
	if err != nil {
		t.Fatalf("inspect external Kafka consumer groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatal("external Kafka has consumer groups; create a fresh broker per test")
	}
	return []string{address}
}
