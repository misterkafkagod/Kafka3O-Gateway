//go:build integration

// Package kafkatest starts a disposable single-node Kafka for integration tests.
package kafkatest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image is the Kafka image used by integration tests.
const Image = "apache/kafka:4.3.1"

const (
	externalPort = "9092/tcp"
	startScript  = "/tmp/kafka3o_start.sh"
)

// Start runs Kafka in KRaft mode and returns its bootstrap address.
// The container is removed when the test finishes.
func Start(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts(externalPort),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                     "a3Fma2EzbzEtdGVzdC0wMQ",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		// The advertised listener must name the host port, which is only known
		// after the container starts, so the container waits for a start script
		// that the post-start hook writes.
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+startScript+" ]; do sleep 0.1; done; exec sh "+startScript),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{writeStartScript},
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", Image, err)
	}

	endpoint, err := ctr.PortEndpoint(ctx, externalPort, "")
	if err != nil {
		t.Fatalf("kafka endpoint: %v", err)
	}
	return []string{endpoint}
}

func writeStartScript(ctx context.Context, c testcontainers.Container) error {
	if err := wait.ForMappedPort(externalPort).WaitUntilReady(ctx, c); err != nil {
		return fmt.Errorf("wait for mapped port: %w", err)
	}
	endpoint, err := c.PortEndpoint(ctx, externalPort, "PLAINTEXT")
	if err != nil {
		return fmt.Errorf("port endpoint: %w", err)
	}
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	script := fmt.Sprintf("export KAFKA_ADVERTISED_LISTENERS=%s,BROKER://%s:9093\nexec /etc/kafka/docker/run\n",
		endpoint, inspect.Config.Hostname)
	if err := c.CopyToContainer(ctx, []byte(script), startScript, 0o755); err != nil {
		return fmt.Errorf("copy start script: %w", err)
	}
	return wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, c)
}
