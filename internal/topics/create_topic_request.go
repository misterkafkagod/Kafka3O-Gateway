package topics

type CreateTopicRequest struct {
	Name              string `json:"name" required:"true" doc:"Kafka topic name"`
	Partitions        int32  `json:"partitions" required:"true" doc:"Positive partition count"`
	ReplicationFactor int16  `json:"replicationFactor" required:"true" doc:"Positive replication factor"`
}
