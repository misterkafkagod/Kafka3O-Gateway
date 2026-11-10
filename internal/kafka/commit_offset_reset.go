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

type offsetRangeAdmin interface {
	FetchOffsets(context.Context, string) (kadm.OffsetResponses, error)
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
}

type offsetCommitter interface {
	CommitOffsets(context.Context, string, kadm.Offsets) (kadm.OffsetResponses, error)
}

type commitOffsetReset struct {
	inactive  GroupInactiveCheck
	admin     offsetRangeAdmin
	committer offsetCommitter
}

func NewCommitOffsetReset(inactive GroupInactiveCheck, admin offsetRangeAdmin, committer offsetCommitter) groups.CommitOffsetReset {
	return commitOffsetReset{inactive: inactive, admin: admin, committer: committer}
}

// Execute commits the previewed targets only if the group is still inactive, every committed offset still equals
// the preview's, and every target lies within the partition's current log range.
func (c commitOffsetReset) Execute(ctx context.Context, group, topic string, partitions []groups.OffsetResetRequest) (groups.OffsetResetResult, error) {
	if group == "" || topic == "" || len(partitions) == 0 {
		return groups.OffsetResetResult{}, groups.ErrInvalidResetTarget
	}
	if err := c.inactive.Execute(ctx, group); err != nil {
		return groups.OffsetResetResult{}, err
	}
	committed, err := c.admin.FetchOffsets(ctx, group)
	if err != nil {
		return groups.OffsetResetResult{}, fmt.Errorf("fetch committed offsets: %w", err)
	}
	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil && len(starts) == 0 {
		return groups.OffsetResetResult{}, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil && len(ends) == 0 {
		return groups.OffsetResetResult{}, fmt.Errorf("list end offsets: %w", err)
	}
	if missing, ok := ends[topic][-1]; ok && errors.Is(missing.Err, kerr.UnknownTopicOrPartition) || len(ends[topic]) == 0 {
		return groups.OffsetResetResult{}, groups.ErrTopicNotFound
	}

	offsets := kadm.Offsets{}
	seen := map[int32]bool{}
	for _, request := range partitions {
		start, startOK := starts[topic][request.Partition]
		end, endOK := ends[topic][request.Partition]
		if seen[request.Partition] || !startOK || !endOK || start.Err != nil || end.Err != nil {
			return groups.OffsetResetResult{}, groups.ErrInvalidResetTarget
		}
		seen[request.Partition] = true
		var current *int64
		if commit, ok := committed.Lookup(topic, request.Partition); ok && commit.Err == nil && commit.At >= 0 {
			current = &commit.At
		}
		if (current == nil) != (request.ExpectedOffset == nil) || (current != nil && *current != *request.ExpectedOffset) ||
			request.TargetOffset < start.Offset || request.TargetOffset > end.Offset {
			return groups.OffsetResetResult{}, groups.ErrStalePreview
		}
		offsets.Add(kadm.Offset{Topic: topic, Partition: request.Partition, At: request.TargetOffset, LeaderEpoch: -1})
	}

	responses, err := c.committer.CommitOffsets(ctx, group, offsets)
	if err != nil {
		// The request may have reached Kafka, so the reset is neither confirmed nor ruled out.
		return groups.OffsetResetResult{}, fmt.Errorf("%w: %w", groups.ErrOutcomeUnknown, err)
	}
	result := groups.OffsetResetResult{Group: group, Topic: topic, Partitions: make([]groups.PartitionOffsetReset, 0, len(partitions))}
	var failures []error
	for _, request := range partitions {
		response, ok := responses.Lookup(topic, request.Partition)
		if !ok || response.Err != nil {
			failures = append(failures, fmt.Errorf("partition %d: %w", request.Partition, errors.Join(response.Err, errors.New("no commit response"))))
			continue
		}
		target := request.TargetOffset
		result.Partitions = append(result.Partitions, groups.PartitionOffsetReset{Partition: request.Partition, CurrentOffset: &target, TargetOffset: target})
	}
	switch {
	case len(failures) == 0:
		slices.SortFunc(result.Partitions, groups.ComparePartitionOffsetResets)
		return result, nil
	case len(result.Partitions) > 0:
		// Some partitions moved and others did not; report the reset as unconfirmed rather than failed.
		return groups.OffsetResetResult{}, fmt.Errorf("%w: %w", groups.ErrOutcomeUnknown, errors.Join(failures...))
	case errors.Is(errors.Join(failures...), kerr.UnknownMemberID) || errors.Is(errors.Join(failures...), kerr.RebalanceInProgress) ||
		errors.Is(errors.Join(failures...), kerr.IllegalGeneration):
		return groups.OffsetResetResult{}, groups.ErrGroupActive
	default:
		return groups.OffsetResetResult{}, fmt.Errorf("commit offsets: %w", errors.Join(failures...))
	}
}
