package kafka

import (
	"bytes"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func topicMessageFromRecord(record *kgo.Record) topics.TopicMessage {
	headers := make([]topics.TopicMessageHeader, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, topics.TopicMessageHeader{
			Key:   header.Key,
			Value: bytes.Clone(header.Value),
		})
	}
	return topics.TopicMessage{
		Partition:   record.Partition,
		Offset:      record.Offset,
		OffsetExact: strconv.FormatInt(record.Offset, 10),
		Timestamp:   record.Timestamp,
		Key:         bytes.Clone(record.Key),
		Value:       bytes.Clone(record.Value),
		Headers:     headers,
	}
}
