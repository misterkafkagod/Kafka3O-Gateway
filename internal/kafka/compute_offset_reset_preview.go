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
	FetchOffsets(context.Context, string) (kadm.OffsetResponses, error)
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(context.Context, int64, ...string) (kadm.ListedOffsets, error)
}

type computeOffsetResetPreview struct {
	inactive GroupInactiveCheck
	admin    offsetResetAdmin
}

func NewComputeOffsetResetPreview(inactive GroupInactiveCheck, admin offsetResetAdmin) groups.ComputeOffsetResetPreview {
	return computeOffsetResetPreview{inactive: inactive, admin: admin}
}

func (p computeOffsetResetPreview) Execute(ctx context.Context, group, topic string, target groups.OffsetResetTarget) (groups.OffsetResetPreview, error) {
	if group == "" || topic == "" || (target.Kind != "earliest" && target.Kind != "latest" && target.Kind != "timestamp") ||
		(target.Kind == "timestamp" && target.Timestamp < 0) {
		return groups.OffsetResetPreview{}, groups.ErrInvalidResetTarget
	}
	if err := p.inactive.Execute(ctx, group); err != nil {
		return groups.OffsetResetPreview{}, err
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
