package httpapi

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"

type IncreasePartitionsInput struct {
	Topic string `path:"topic" doc:"Topic name"`
	Body  struct {
		Count int32 `json:"count" minimum:"1" doc:"Desired total partition count"`
	}
}

type ReassignPartitionInput struct {
	Topic       string `path:"topic" doc:"Topic name"`
	PartitionID int32  `path:"partitionId" minimum:"0" doc:"Partition ID"`
	Body        struct {
		Replicas []int32 `json:"replicas" minItems:"1" doc:"Ordered broker IDs"`
	}
}

type PartitionChangeOutput struct {
	Body struct {
		Topic  string `json:"topic"`
		Status string `json:"status"`
	}
}

type ReassignmentsOutput struct {
	Body []partitions.Reassignment
}
