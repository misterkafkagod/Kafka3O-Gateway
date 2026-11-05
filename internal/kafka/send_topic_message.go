package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type syncProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type sendTopicMessage struct {
	producer syncProducer
}

func NewSendTopicMessage(producer syncProducer) topics.SendTopicMessage {
	return sendTopicMessage{producer: producer}
}

func (s sendTopicMessage) Execute(ctx context.Context, topic string, request topics.ProduceMessageRequest) (topics.ProduceMessageResult, error) {
	record := &kgo.Record{Topic: topic, Value: []byte(*request.Value)}
	if request.Key != nil {
		record.Key = []byte(*request.Key)
	}
	for _, header := range request.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: *header.Key, Value: []byte(*header.Value)})
	}
	produced, err := s.producer.ProduceSync(ctx, record).First()
	if err != nil {
		return topics.ProduceMessageResult{}, fmt.Errorf("produce message: %w", err)
	}
	if produced == nil || produced.Partition < 0 || produced.Offset < 0 {
		return topics.ProduceMessageResult{}, errors.New("broker did not confirm a message position")
	}
	return topics.ProduceMessageResult{Partition: produced.Partition, Offset: strconv.FormatInt(produced.Offset, 10)}, nil
}
