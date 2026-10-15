package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/api"
)

// ListTopics returns every topic, internal ones included, with offsets.
func (c *Client) ListTopics(ctx context.Context) ([]api.TopicPartitions, error) {
	td, err := c.adm.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, err
	}
	return c.withOffsets(ctx, td)
}

// DescribeTopic returns one topic's partitions, offsets and configuration.
func (c *Client) DescribeTopic(ctx context.Context, name string) (api.TopicPartitions, []api.ConfigEntry, error) {
	td, err := c.adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return api.TopicPartitions{}, nil, err
	}
	if t, ok := td[name]; !ok || errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
		return api.TopicPartitions{}, nil, fmt.Errorf("%w: %s", api.ErrTopicNotFound, name)
	} else if t.Err != nil {
		return api.TopicPartitions{}, nil, t.Err
	}

	topics, err := c.withOffsets(ctx, td)
	if err != nil {
		return api.TopicPartitions{}, nil, err
	}

	rcs, err := c.adm.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return api.TopicPartitions{}, nil, err
	}
	rc, err := rcs.On(name, nil)
	if err == nil {
		err = rc.Err
	}
	if err != nil {
		return api.TopicPartitions{}, nil, err
	}

	configs := make([]api.ConfigEntry, 0, len(rc.Configs))
	for _, cfg := range rc.Configs {
		configs = append(configs, api.ConfigEntry{
			Key:   cfg.Key,
			Value: cfg.Value,
			// Only a value set on the topic itself counts as an override.
			IsDefault: cfg.Source != kmsg.ConfigSourceDynamicTopicConfig,
			Sensitive: cfg.Sensitive,
		})
	}
	return topics[0], configs, nil
}

// withOffsets converts topic metadata to api types, adding start and end offsets.
func (c *Client) withOffsets(ctx context.Context, td kadm.TopicDetails) ([]api.TopicPartitions, error) {
	if len(td) == 0 {
		return []api.TopicPartitions{}, nil
	}
	names := td.Names()
	starts, err := c.adm.ListStartOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := c.adm.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	out := make([]api.TopicPartitions, 0, len(td))
	for _, t := range td.Sorted() {
		tp := api.TopicPartitions{Name: t.Topic, Partitions: make([]api.PartitionInfo, 0, len(t.Partitions))}
		for _, p := range t.Partitions.Sorted() {
			info := api.PartitionInfo{ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR}
			if o, ok := starts.Lookup(t.Topic, p.Partition); ok && o.Err == nil {
				info.StartOffset = o.Offset
			}
			if o, ok := ends.Lookup(t.Topic, p.Partition); ok && o.Err == nil {
				info.EndOffset = o.Offset
			}
			tp.Partitions = append(tp.Partitions, info)
		}
		out = append(out, tp)
	}
	return out, nil
}
