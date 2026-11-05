//go:build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestProduceTopicMessage_ReadsBackBytesAndPosition(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	producer, err := kafka.NewProducer(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	value, key, headerKey, headerValue := "café", "", "source", "integration"
	request := topics.ProduceMessageRequest{
		Value: &value, Key: &key, Headers: []topics.ProduceMessageHeader{{Key: &headerKey, Value: &headerValue}},
	}
	position, err := topics.NewProduceTopicMessage(kafka.NewSendTopicMessage(producer), 30*time.Second).Execute(ctx, "orders", request)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := strconv.ParseInt(position.Offset, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, client), 30*time.Second).
		Execute(ctx, "orders", topics.TopicMessagePosition{Partition: position.Partition, Offset: &offset})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].OffsetExact != position.Offset || messages[0].Partition != position.Partition ||
		messages[0].Key == nil || len(messages[0].Key) != 0 || string(messages[0].Value) != value ||
		!cmp.Equal(messages[0].Headers, []topics.TopicMessageHeader{{Key: headerKey, Value: []byte(headerValue)}}) {
		t.Fatalf("produced position = %#v, messages = %#v", position, messages)
	}
}
