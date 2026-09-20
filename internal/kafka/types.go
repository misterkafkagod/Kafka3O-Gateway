// Package kafka is the gateway's port to the managed cluster (TECH-SPEC §2.2):
// domain types, a typed error, and three role interfaces — Admin, Consumer,
// Producer — mirroring the protocol surfaces of FUNC-SPEC §8.1.
//
// The package imports only the standard library (TECH-SPEC D1). No franz-go
// type crosses this boundary; adapters in kafka/franz and kafka/fake translate.
// Domain types carry no JSON, HTTP, or validation tags (TECH-SPEC I5): the API
// layer owns wire shapes.
package kafka

import "time"

// Broker is one cluster member (FUNC-SPEC §8.7 C1).
type Broker struct {
	ID   int32
	Host string
	Port int32
	Rack string
}

// ClusterInfo is the result of describing the cluster (FUNC-SPEC §8.7 C1).
type ClusterInfo struct {
	ClusterID    string
	ControllerID int32
	Brokers      []Broker
}

// Topic describes one topic and its partitions (FUNC-SPEC §8.7 T1, T2).
type Topic struct {
	Name              string
	Internal          bool
	ReplicationFactor int
	Partitions        []Partition
}

// Partition is one partition's leadership, replication, and offset range.
// EndOffset is exclusive: the next offset to be written.
type Partition struct {
	ID          int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
	BeginOffset int64
	EndOffset   int64
}

// TopicPartition names one partition of one topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Record is one message as stored: raw bytes, undecoded. Decoding into
// string / JSON / base64 is the scan layer's job (FUNC-SPEC §8.2).
// A nil Value is a tombstone (FUNC-SPEC §8.7 M7).
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Header is one record header; the value is raw bytes.
type Header struct {
	Key   string
	Value []byte
}

// Group describes a consumer group (FUNC-SPEC §8.7 G1, G2).
type Group struct {
	ID            string
	State         string
	ProtocolType  string
	CoordinatorID int32
	Members       []GroupMember
}

// GroupMember is one live member of a consumer group and its assignment.
type GroupMember struct {
	MemberID    string
	ClientID    string
	Host        string
	Assignments []TopicPartition
}

// ConfigSource is the normalised origin of a configuration value
// (FUNC-SPEC §5.1 T2: default / static / dynamic).
type ConfigSource string

// ConfigSource values.
const (
	SourceDefault ConfigSource = "default"
	SourceStatic  ConfigSource = "static"
	SourceDynamic ConfigSource = "dynamic"
)

// ConfigEntry is one broker or topic configuration property (FUNC-SPEC §8.7 C2, T2).
// Adapters blank Value when IsSensitive is set; the API layer renders it as null.
type ConfigEntry struct {
	Name        string
	Value       string
	Source      ConfigSource
	IsSensitive bool
	IsReadOnly  bool
}
