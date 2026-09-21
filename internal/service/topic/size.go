package topic

import (
	"context"
	"sort"
)

// ReplicaSize is one replica's on-disk footprint (FUNC-SPEC §8.7 T3).
type ReplicaSize struct {
	BrokerID int32
	LogDir   string
	Bytes    int64
}

// PartitionSize is one partition's total on-disk footprint across every
// replica, and the per-replica breakdown (FUNC-SPEC §8.7 T3).
type PartitionSize struct {
	ID       int32
	Bytes    int64
	Replicas []ReplicaSize
}

// Size is Size's result (FUNC-SPEC §8.7 T3). TotalBytes sums every replica of
// every partition — the topic's total on-disk footprint across the cluster,
// replication included.
type Size struct {
	Topic      string
	TotalBytes int64
	Partitions []PartitionSize
}

// Size returns per-partition, per-replica on-disk byte sizes for name
// (FUNC-SPEC §8.7 T3). Unknown topic → the port's NotFound error, unchanged.
func (s *Service) Size(ctx context.Context, name string) (Size, error) {
	entries, err := s.admin.DescribeLogDirs(ctx, name)
	if err != nil {
		return Size{}, err
	}

	byPartition := map[int32][]ReplicaSize{}
	for _, e := range entries {
		byPartition[e.Partition] = append(byPartition[e.Partition], ReplicaSize{
			BrokerID: e.BrokerID, LogDir: e.LogDir, Bytes: e.Bytes,
		})
	}

	var total int64
	partitions := make([]PartitionSize, 0, len(byPartition))
	for id, replicas := range byPartition {
		sort.Slice(replicas, func(i, j int) bool { return replicas[i].BrokerID < replicas[j].BrokerID })
		var pBytes int64
		for _, r := range replicas {
			pBytes += r.Bytes
		}
		total += pBytes
		partitions = append(partitions, PartitionSize{ID: id, Bytes: pBytes, Replicas: replicas})
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })

	return Size{Topic: name, TotalBytes: total, Partitions: partitions}, nil
}
