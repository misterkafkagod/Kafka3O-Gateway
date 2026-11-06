package partitions

import "errors"

type Placement struct {
	Brokers    []int32
	Partitions map[int32][]int32
}

type Reassignment struct {
	Partition        int32   `json:"partition"`
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"addingReplicas"`
	RemovingReplicas []int32 `json:"removingReplicas"`
}

var (
	ErrInvalidChange = errors.New("invalid partition change")
	ErrNotFound      = errors.New("topic or partition not found")
	ErrDenied        = errors.New("partition operation denied")
	ErrRejected      = errors.New("partition change rejected")
	ErrInProgress    = errors.New("partition reassignment already in progress")
	ErrWriteUnknown  = errors.New("partition change outcome unknown")
)
