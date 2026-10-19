//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	kafkaImage     = "apache/kafka:4.3.1"
	kafkaClusterID = "4L6g3nShT-eMCtK--X86sw"
	startScript    = "/tmp/kafka3o_start.sh"
)

// StartKafka runs a single-node KRaft apache/kafka container and returns its bootstrap address.
// The container waits for a start script; a post-start hook writes it with the advertised
// listener set to the real host and mapped port, then runs the image's own /etc/kafka/docker/run.
func StartKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                     kafkaClusterID,
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		testcontainers.WithEntrypoint("bash"),
		testcontainers.WithCmd("-c", "while [ ! -f "+startScript+" ]; do sleep 0.1; done; exec bash "+startScript),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					host, err := c.Host(ctx)
					if err != nil {
						return fmt.Errorf("container host: %w", err)
					}
					port, err := c.MappedPort(ctx, "9092/tcp")
					if err != nil {
						return fmt.Errorf("mapped port: %w", err)
					}
					script := fmt.Sprintf("#!/bin/bash\nexport KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s:%s,BROKER://localhost:9094\nexec /etc/kafka/docker/run\n", host, port.Port())
					if err := c.CopyToContainer(ctx, []byte(script), startScript, 0o755); err != nil {
						return fmt.Errorf("copy start script: %w", err)
					}
					return wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, c)
				},
			},
		}),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", kafkaImage, err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return []string{fmt.Sprintf("%s:%s", host, port.Port())}
}
