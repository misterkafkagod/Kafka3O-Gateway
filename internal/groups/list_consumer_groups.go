package groups

import (
	"context"
	"slices"
	"time"
)

type ListConsumerGroups interface {
	Execute(ctx context.Context) ([]ConsumerGroup, error)
}

type listConsumerGroups struct {
	fetch   FetchConsumerGroups
	timeout time.Duration
}

func NewListConsumerGroups(fetch FetchConsumerGroups, timeout time.Duration) ListConsumerGroups {
	return listConsumerGroups{fetch: fetch, timeout: timeout}
}

func (l listConsumerGroups) Execute(ctx context.Context) ([]ConsumerGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	snapshots, err := l.fetch.Execute(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]ConsumerGroup, 0, len(snapshots))
	for _, snapshot := range snapshots {
		members := append([]ConsumerGroupMember{}, snapshot.Members...)
		slices.SortFunc(members, compareConsumerGroupMembers)
		topics := append([]string{}, snapshot.Topics...)
		slices.Sort(topics)
		topics = slices.Compact(topics)
		partitions := make([]ConsumerGroupPartition, 0, len(snapshot.Partitions))
		for _, partition := range snapshot.Partitions {
			partitions = append(partitions, calculateConsumerPartitionLag(partition))
		}
		slices.SortFunc(partitions, compareConsumerGroupPartitions)
		result = append(result, ConsumerGroup{
			ID: snapshot.ID, Protocol: snapshot.Protocol, State: snapshot.State,
			Members: members, Topics: topics, Partitions: partitions, Error: snapshot.Error,
		})
	}
	slices.SortFunc(result, compareConsumerGroups)
	return result, nil
}
