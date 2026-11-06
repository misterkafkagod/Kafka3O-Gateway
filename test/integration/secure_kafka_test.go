//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestKafkaSecurity_RealBroker(t *testing.T) {
	broker, configured := os.LookupEnv("KAFKA3O_TEST_SECURE_BROKER")
	if !configured {
		t.Skip("requires a fresh disposable secured Kafka broker")
	}
	disposable := os.Getenv("KAFKA3O_TEST_DISPOSABLE")
	if err := ValidateExternalTestBroker(broker, disposable); err != nil {
		t.Fatal(err)
	}
	tlsBroker := os.Getenv("KAFKA3O_TEST_TLS_BROKER")
	saslBroker := os.Getenv("KAFKA3O_TEST_SASL_BROKER")
	for _, address := range []string{tlsBroker, saslBroker} {
		if err := ValidateExternalTestBroker(address, disposable); err != nil {
			t.Fatal(err)
		}
	}
	caFile := os.Getenv("KAFKA3O_TEST_SECURE_CA")
	username := os.Getenv("KAFKA3O_TEST_SECURE_USERNAME")
	if caFile == "" || username == "" {
		t.Fatal("secured broker test requires a CA file and username")
	}
	t.Setenv("KAFKA3O_KAFKA__BROKERS", broker)
	t.Setenv("KAFKA3O_KAFKA__TLS__ENABLED", "true")
	t.Setenv("KAFKA3O_KAFKA__TLS__CAFILE", caFile)
	t.Setenv("KAFKA3O_KAFKA__SASL__MECHANISM", "SCRAM-SHA-512")
	t.Setenv("KAFKA3O_KAFKA__SASL__USERNAME", username)
	loaded, err := config.LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	cfg := loaded.Kafka
	client, err := kafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	VerifyExternalTestBroker(t, client, broker, disposable)

	cases := []struct {
		name      string
		broker    string
		tls       bool
		mechanism string
	}{
		{name: "TLS and PLAIN", broker: broker, tls: true, mechanism: "PLAIN"},
		{name: "TLS and SCRAM-SHA-256", broker: broker, tls: true, mechanism: "SCRAM-SHA-256"},
		{name: "TLS and SCRAM-SHA-512", broker: broker, tls: true, mechanism: "SCRAM-SHA-512"},
		{name: "TLS only", broker: tlsBroker, tls: true},
		{name: "PLAIN only", broker: saslBroker, mechanism: "PLAIN"},
		{name: "SCRAM-SHA-256 only", broker: saslBroker, mechanism: "SCRAM-SHA-256"},
		{name: "SCRAM-SHA-512 only", broker: saslBroker, mechanism: "SCRAM-SHA-512"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode := cfg
			mode.Brokers = []string{tc.broker}
			if !tc.tls {
				mode.TLS = config.KafkaTLSConfig{}
			}
			if tc.mechanism == "" {
				mode.SASL = config.KafkaSASLConfig{}
			} else {
				mode.SASL.Mechanism = tc.mechanism
			}
			testClient, err := kafka.NewClient(mode)
			if err != nil {
				t.Fatal(err)
			}
			defer testClient.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cluster, err := kafka.NewDescribeCluster(testClient).Execute(ctx)
			if err != nil {
				t.Fatalf("secured metadata request: %v", err)
			}
			host, port, _ := net.SplitHostPort(tc.broker)
			if cluster.ID != kafkaClusterID || cluster.ControllerID != 1 || len(cluster.Brokers) != 1 ||
				cluster.Brokers[0].ID != 1 || cluster.Brokers[0].Host != host ||
				strconv.Itoa(int(cluster.Brokers[0].Port)) != port {
				t.Fatal("secured metadata identified an unexpected broker")
			}
		})
	}

	testKafkaSecurityRejections(t, cfg)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(producer).CreateTopic(ctx, 1, 1, nil, "secure-kafka-test"); err != nil {
		t.Fatal(err)
	}
	value := "synthetic-secured-message"
	position, err := topics.NewProduceTopicMessage(kafka.NewSendTopicMessage(producer), 20*time.Second).
		Execute(ctx, "secure-kafka-test", topics.ProduceMessageRequest{Value: &value})
	if err != nil || position.Partition != 0 || position.Offset != "0" {
		t.Fatalf("secured produce position = %#v, error = %v", position, err)
	}

	admin := kadm.NewClient(client)
	latest, err := kafka.NewListLatestTopicMessages(admin, client).Execute(ctx, "secure-kafka-test")
	if err != nil || len(latest) != 1 || string(latest[0].Value) != value {
		t.Fatalf("secured latest read: messages = %d, error = %v", len(latest), err)
	}
	offset := int64(0)
	positioned, err := kafka.NewReadTopicMessages(admin, client).Execute(
		ctx, "secure-kafka-test", topics.TopicMessagePosition{Partition: 0, Offset: &offset},
	)
	if err != nil || len(positioned) != 1 || string(positioned[0].Value) != value {
		t.Fatalf("secured positioned read: messages = %d, error = %v", len(positioned), err)
	}
	window, err := kafka.NewFetchRecentTopicMessages(admin, client).Execute(ctx, "secure-kafka-test")
	if err != nil || len(window.Messages) != 1 || string(window.Messages[0].Value) != value {
		t.Fatalf("secured search window: messages = %d, error = %v", len(window.Messages), err)
	}
}
