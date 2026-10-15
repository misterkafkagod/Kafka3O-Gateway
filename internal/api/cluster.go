// Package api serves the gateway's HTTP API. Handlers depend only on the
// Cluster interface; the Kafka adapter in internal/kafka implements it.
package api

import (
	"context"
	"errors"
)

// Cluster is everything the HTTP layer needs from Kafka.
type Cluster interface {
	// Ping checks that the cluster is reachable.
	Ping(ctx context.Context) error
	// DescribeCluster returns the cluster id, controller and brokers.
	DescribeCluster(ctx context.Context) (ClusterInfo, error)
	// ListTopics returns every topic with its partitions and offsets.
	ListTopics(ctx context.Context) ([]TopicPartitions, error)
	// DescribeTopic returns one topic's partitions and configuration.
	// It returns ErrTopicNotFound if the topic does not exist.
	DescribeTopic(ctx context.Context, name string) (TopicPartitions, []ConfigEntry, error)
}

// ErrTopicNotFound is returned by Cluster methods for a topic that does not exist.
var ErrTopicNotFound = errors.New("topic not found")

// ClusterInfo describes the cluster as a whole.
type ClusterInfo struct {
	ClusterID    string   `json:"clusterId"`
	ControllerID int32    `json:"controllerId"`
	Brokers      []Broker `json:"brokers"`
}

// Broker is one broker of the cluster.
type Broker struct {
	ID   int32   `json:"id"`
	Host string  `json:"host"`
	Port int32   `json:"port"`
	Rack *string `json:"rack"`
}

// TopicPartitions is a topic as read from Kafka, before any derived fields.
type TopicPartitions struct {
	Name       string
	Partitions []PartitionInfo
}

// PartitionInfo is one partition's replicas and offsets as read from Kafka.
type PartitionInfo struct {
	ID          int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
	StartOffset int64
	EndOffset   int64
}

// ConfigEntry is one topic configuration value as read from Kafka.
type ConfigEntry struct {
	Key       string
	Value     *string
	IsDefault bool
	Sensitive bool
}
