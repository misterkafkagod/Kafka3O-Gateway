package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type partitionHealthAdmin interface {
	ListTopicsWithInternal(context.Context, ...string) (kadm.TopicDetails, error)
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
}

type readPartitionHealth struct {
	admin partitionHealthAdmin
}

// NewReadPartitionHealth adapts metadata and topic configuration to the metrics.ReadPartitionHealth port.
func NewReadPartitionHealth(admin partitionHealthAdmin) metrics.ReadPartitionHealth {
	return readPartitionHealth{admin: admin}
}

func (r readPartitionHealth) Execute(ctx context.Context) ([]metrics.TopicHealth, error) {
	details, err := r.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}
	minISR := map[string]int{}
	minISRErrors := map[string]string{}
	configs, configErr := r.admin.DescribeTopicConfigs(ctx, details.Names()...)
	for _, resource := range configs {
		if resource.Err != nil {
			minISRErrors[resource.Name] = resource.Err.Error()
			continue
		}
		for _, config := range resource.Configs {
			if config.Key != "min.insync.replicas" || config.Value == nil {
				continue
			}
			if value, parseErr := strconv.Atoi(*config.Value); parseErr == nil {
				minISR[resource.Name] = value
			}
		}
	}
	health := make([]metrics.TopicHealth, 0, len(details))
	for _, detail := range details {
		topic := metrics.TopicHealth{Topic: detail.Topic}
		if detail.Err != nil {
			topic.Error = detail.Err.Error()
			health = append(health, topic)
			continue
		}
		required, known := minISR[detail.Topic]
		switch {
		case configErr != nil:
			topic.MinISRError = fmt.Sprintf("read topic configuration: %v", configErr)
		case minISRErrors[detail.Topic] != "":
			topic.MinISRError = minISRErrors[detail.Topic]
		case !known:
			topic.MinISRError = "min.insync.replicas was not reported"
		default:
			topic.BelowMinISR = new(int)
		}
		for _, partition := range detail.Partitions {
			topic.Partitions++
			if partition.Leader < 0 || partition.Err != nil {
				topic.Offline++
			}
			if len(partition.ISR) < len(partition.Replicas) {
				topic.UnderReplicated++
			}
			if topic.BelowMinISR != nil && len(partition.ISR) < required {
				*topic.BelowMinISR++
			}
		}
		health = append(health, topic)
	}
	return health, nil
}
