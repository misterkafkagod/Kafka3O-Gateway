//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
)

func TestPersistentKafka_Security(t *testing.T) {
	cfg, ip, number := loadPersistentKafkaConfig(t)

	client, err := kafka.NewClient(cfg)
	if err != nil {
		t.Fatalf("create persistent broker client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	cluster, err := kafka.NewDescribeCluster(client).Execute(ctx)
	cancel()
	if err != nil {
		t.Fatalf("persistent broker metadata failed (error type %T)", err)
	}
	if cluster.ID == "" || cluster.ControllerID != 1 || len(cluster.Brokers) != 1 ||
		cluster.Brokers[0].ID != 1 || !net.ParseIP(cluster.Brokers[0].Host).Equal(ip) ||
		uint64(cluster.Brokers[0].Port) != number {
		t.Fatal("persistent broker metadata did not match the configured loopback listener")
	}

	testKafkaSecurityRejections(t, cfg)
}

func loadPersistentKafkaConfig(t *testing.T) (config.KafkaConfig, net.IP, uint64) {
	t.Helper()
	broker, configured := os.LookupEnv("KAFKA3O_TEST_PERSISTENT_BROKER")
	if !configured {
		t.Skip("requires an explicitly configured persistent Kafka broker")
	}
	host, port, err := net.SplitHostPort(broker)
	ip := net.ParseIP(host)
	number, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || ip == nil || !ip.IsLoopback() || portErr != nil || number == 0 {
		t.Fatal("KAFKA3O_TEST_PERSISTENT_BROKER must be a numeric loopback address and valid TCP port")
	}
	t.Setenv("KAFKA3O_KAFKA__BROKERS", broker)
	loaded, err := config.LoadConfig("")
	if err != nil {
		t.Fatalf("load persistent broker configuration: %v", err)
	}
	cfg := loaded.Kafka
	if !cfg.TLS.Enabled || cfg.TLS.CAFile == "" ||
		!strings.EqualFold(strings.TrimSpace(cfg.SASL.Mechanism), "SCRAM-SHA-512") {
		t.Fatal("persistent broker verification requires a CA file, TLS, and SCRAM-SHA-512")
	}
	return cfg, ip, number
}
