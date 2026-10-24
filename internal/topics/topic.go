package topics

// Topic is a topic as listed by the gateway, with its approximate message count.
type Topic struct {
	Name              string `json:"name"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replicationFactor"`
	Internal          bool   `json:"internal"`
	ApproxMessages    int64  `json:"approxMessages"`
}

// TopicMetadata is a topic's shape as read from Kafka metadata.
type TopicMetadata struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	Internal          bool
}
