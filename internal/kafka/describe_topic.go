package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type describeTopic struct {
	admin *kadm.Client
}

func NewDescribeTopic(admin *kadm.Client) topics.DescribeTopic {
	return describeTopic{admin: admin}
}

func (d describeTopic) Execute(ctx context.Context, name string) (topics.TopicDetail, error) {
	details, err := d.admin.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return topics.TopicDetail{}, fmt.Errorf("load topic metadata: %w", err)
	}
	detail, ok := details[name]
	if !ok || detail.Err == kerr.UnknownTopicOrPartition {
		return topics.TopicDetail{}, topics.ErrTopicNotFound
	}
	if detail.Err != nil {
		return topics.TopicDetail{}, fmt.Errorf("load topic metadata: %w", detail.Err)
	}

	starts, err := d.admin.ListStartOffsets(ctx, name)
	if err != nil {
		return topics.TopicDetail{}, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, d.admin, name, err))
	}
	if err := starts.Error(); err != nil {
		return topics.TopicDetail{}, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, d.admin, name, err))
	}
	ends, err := d.admin.ListEndOffsets(ctx, name)
	if err != nil {
		return topics.TopicDetail{}, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, d.admin, name, err))
	}
	if err := ends.Error(); err != nil {
		return topics.TopicDetail{}, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, d.admin, name, err))
	}

	result := topics.TopicDetail{
		Name:       name,
		Partitions: make([]topics.TopicPartition, 0, len(detail.Partitions)),
	}
	for _, partition := range detail.Partitions.Sorted() {
		start, ok := starts.Lookup(name, partition.Partition)
		if !ok {
			return topics.TopicDetail{}, fmt.Errorf("missing start offset for partition %d", partition.Partition)
		}
		end, ok := ends.Lookup(name, partition.Partition)
		if !ok {
			return topics.TopicDetail{}, fmt.Errorf("missing end offset for partition %d", partition.Partition)
		}
		result.Partitions = append(result.Partitions, topics.TopicPartition{
			ID:          partition.Partition,
			Leader:      partition.Leader,
			Replicas:    partition.Replicas,
			ISR:         partition.ISR,
			StartOffset: start.Offset,
			EndOffset:   end.Offset,
		})
	}

	configs, err := d.admin.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return topics.TopicDetail{}, fmt.Errorf("describe topic config: %w", err)
	}
	config, err := configs.On(name, nil)
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return topics.TopicDetail{}, topics.ErrTopicNotFound
	}
	if err != nil {
		return topics.TopicDetail{}, fmt.Errorf("describe topic config: %w", err)
	}
	if config.Err != nil {
		if errors.Is(config.Err, kerr.UnknownTopicOrPartition) {
			return topics.TopicDetail{}, topics.ErrTopicNotFound
		}
		return topics.TopicDetail{}, fmt.Errorf("describe topic config: %w", config.Err)
	}
	result.Config = make([]topics.TopicConfig, 0, len(config.Configs))
	for _, entry := range config.Configs {
		value := ""
		if !entry.Sensitive && entry.Value != nil {
			value = *entry.Value
		}
		result.Config = append(result.Config, topics.TopicConfig{
			Name:      entry.Key,
			Value:     value,
			Sensitive: entry.Sensitive,
		})
	}
	slices.SortFunc(result.Config, func(a, b topics.TopicConfig) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result, nil
}
