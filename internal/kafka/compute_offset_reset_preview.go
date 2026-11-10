package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type offsetResetAdmin interface {
	ListGroupsByType(context.Context, []string, ...string) (kadm.ListedGroups, error)
	DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error)
	DescribeConsumerGroups(context.Context, ...string) (kadm.DescribedConsumerGroups, error)
	FetchOffsets(context.Context, string) (kadm.OffsetResponses, error)
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(context.Context, int64, ...string) (kadm.ListedOffsets, error)
}

type computeOffsetResetPreview struct {
	admin offsetResetAdmin
}

func NewComputeOffsetResetPreview(admin offsetResetAdmin) groups.ComputeOffsetResetPreview {
	return computeOffsetResetPreview{admin: admin}
}

func (p computeOffsetResetPreview) Execute(ctx context.Context, group, topic string, target groups.OffsetResetTarget) (groups.OffsetResetPreview, error) {
	if group == "" || topic == "" || (target.Kind != "earliest" && target.Kind != "latest" && target.Kind != "timestamp") ||
		(target.Kind == "timestamp" && target.Timestamp < 0) {
		return groups.OffsetResetPreview{}, groups.ErrInvalidResetTarget
	}
	classic, err := p.admin.ListGroupsByType(ctx, []string{"classic"})
	if err != nil {
		return groups.OffsetResetPreview{}, fmt.Errorf("list classic groups: %w", err)
	}
	modern, err := p.admin.ListGroupsByType(ctx, []string{"consumer"})
	if err != nil {
		return groups.OffsetResetPreview{}, fmt.Errorf("list consumer groups: %w", err)
	}
	// A reset requires a group without members; Kafka would also reject commits from outside an active group.
	if listed, ok := classic[group]; ok && (listed.ProtocolType == "" || listed.ProtocolType == "consumer") {
		described, err := p.admin.DescribeGroups(ctx, group)
		if err != nil && len(described) == 0 {
			return groups.OffsetResetPreview{}, fmt.Errorf("describe group: %w", err)
		}
		entry, ok := described[group]
		if !ok || entry.Err != nil {
			return groups.OffsetResetPreview{}, fmt.Errorf("describe group: %w", errors.Join(entry.Err, err))
		}
		if len(entry.Members) > 0 {
			return groups.OffsetResetPreview{}, groups.ErrGroupActive
		}
	} else if _, ok := modern[group]; ok {
		described, err := p.admin.DescribeConsumerGroups(ctx, group)
		if err != nil && len(described) == 0 {
			return groups.OffsetResetPreview{}, fmt.Errorf("describe consumer group: %w", err)
		}
		entry, ok := described[group]
		if !ok || entry.Err != nil {
			return groups.OffsetResetPreview{}, fmt.Errorf("describe consumer group: %w", errors.Join(entry.Err, err))
		}
		if len(entry.Members) > 0 {
			return groups.OffsetResetPreview{}, groups.ErrGroupActive
		}
	} else {
		return groups.OffsetResetPreview{}, groups.ErrGroupNotFound
	}

	committed, err := p.admin.FetchOffsets(ctx, group)
	if err != nil {
		return groups.OffsetResetPreview{}, fmt.Errorf("fetch committed offsets: %w", err)
	}
	ends, err := p.admin.ListEndOffsets(ctx, topic)
	if err != nil && len(ends) == 0 {
		return groups.OffsetResetPreview{}, fmt.Errorf("list end offsets: %w", err)
	}
	if missing, ok := ends[topic][-1]; ok && errors.Is(missing.Err, kerr.UnknownTopicOrPartition) || len(ends[topic]) == 0 {
		return groups.OffsetResetPreview{}, groups.ErrTopicNotFound
	}
	targets := ends
	switch target.Kind {
	case "earliest":
		targets, err = p.admin.ListStartOffsets(ctx, topic)
	case "timestamp":
		targets, err = p.admin.ListOffsetsAfterMilli(ctx, target.Timestamp, topic)
	}
	if err != nil && len(targets) == 0 {
		return groups.OffsetResetPreview{}, fmt.Errorf("list target offsets: %w", err)
	}

	preview := groups.OffsetResetPreview{Group: group, Topic: topic, Partitions: make([]groups.PartitionOffsetReset, 0, len(ends[topic]))}
	for partition, end := range ends[topic] {
		if end.Err != nil {
			return groups.OffsetResetPreview{}, fmt.Errorf("partition %d end offset: %w", partition, end.Err)
		}
		listed, ok := targets[topic][partition]
		if !ok || listed.Err != nil {
			return groups.OffsetResetPreview{}, fmt.Errorf("partition %d target offset: %w", partition, listed.Err)
		}
		offset := listed.Offset
		// A timestamp after the newest record has no matching offset; the reset then targets the end.
		if offset < 0 {
			offset = end.Offset
		}
		reset := groups.PartitionOffsetReset{Partition: partition, TargetOffset: offset}
		if commit, ok := committed.Lookup(topic, partition); ok && commit.Err == nil && commit.At >= 0 {
			current := commit.At
			reset.CurrentOffset = &current
		}
		preview.Partitions = append(preview.Partitions, reset)
	}
	slices.SortFunc(preview.Partitions, groups.ComparePartitionOffsetResets)
	return preview, nil
}
