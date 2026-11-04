package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type consumerGroupsAdmin interface {
	ListGroupsByType(context.Context, []string, ...string) (kadm.ListedGroups, error)
	DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error)
	DescribeConsumerGroups(context.Context, ...string) (kadm.DescribedConsumerGroups, error)
	FetchManyOffsets(context.Context, ...string) kadm.FetchOffsetsResponses
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
}

type fetchConsumerGroups struct {
	admin consumerGroupsAdmin
}

func NewFetchConsumerGroups(admin consumerGroupsAdmin) groups.FetchConsumerGroups {
	return fetchConsumerGroups{admin: admin}
}

func (f fetchConsumerGroups) Execute(ctx context.Context) ([]groups.ConsumerGroupSnapshot, error) {
	classic, err := f.admin.ListGroupsByType(ctx, []string{"classic"})
	if err != nil {
		return nil, fmt.Errorf("list classic groups: %w", err)
	}
	modern, err := f.admin.ListGroupsByType(ctx, []string{"consumer"})
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	for id, listed := range classic {
		if listed.ProtocolType != "" && listed.ProtocolType != "consumer" {
			delete(classic, id)
		}
	}
	for id := range modern {
		if _, ok := classic[id]; ok {
			return nil, fmt.Errorf("group %q was listed with multiple protocols; retry", id)
		}
	}

	var classicDescriptions kadm.DescribedGroups
	var classicErr error
	if len(classic) > 0 {
		classicDescriptions, classicErr = f.admin.DescribeGroups(ctx, classic.Groups()...)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("describe classic groups: %w", err)
	}
	var modernDescriptions kadm.DescribedConsumerGroups
	var modernErr error
	if len(modern) > 0 {
		modernDescriptions, modernErr = f.admin.DescribeConsumerGroups(ctx, modern.Groups()...)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("describe consumer groups: %w", err)
	}
	data := make([]consumerGroupData, 0, len(classic)+len(modern))
	for _, listed := range classic.Sorted() {
		described, ok := classicDescriptions[listed.Group]
		if ok && described.Err == nil && described.ProtocolType != "" && described.ProtocolType != "consumer" {
			continue
		}
		if ok {
			data = append(data, consumerGroupFromClassic(listed, &described, nil))
		} else {
			data = append(data, consumerGroupFromClassic(listed, nil, classicErr))
		}
	}
	for _, listed := range modern.Sorted() {
		described, ok := modernDescriptions[listed.Group]
		if ok {
			data = append(data, consumerGroupFromModern(listed, &described, nil))
		} else {
			data = append(data, consumerGroupFromModern(listed, nil, modernErr))
		}
	}
	if classicErr != nil && len(classicDescriptions) == len(classic) {
		return nil, fmt.Errorf("describe classic groups: %w", classicErr)
	}
	if modernErr != nil && len(modernDescriptions) == len(modern) {
		return nil, fmt.Errorf("describe consumer groups: %w", modernErr)
	}

	names := make([]string, 0, len(data))
	for _, group := range data {
		names = append(names, group.snapshot.ID)
	}
	if len(names) == 0 {
		return []groups.ConsumerGroupSnapshot{}, nil
	}
	fetched := f.admin.FetchManyOffsets(ctx, names...)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	allTopics := make(kadm.TopicsSet)
	for i := range data {
		group := &data[i]
		response, ok := fetched[group.snapshot.ID]
		switch {
		case !ok:
			group.commitError = errors.New("missing group offset response")
		case response.Err != nil:
			group.commitError = response.Err
		default:
			group.commits = response.Fetched
			for topic, partitions := range response.Fetched {
				for partition, offset := range partitions {
					if offset.At != -1 || offset.Err != nil {
						group.topics.Add(topic, partition)
					}
				}
			}
		}
		allTopics.MergeTopics(group.topics.Topics())
	}

	var starts, ends kadm.ListedOffsets
	var startErr, endErr error
	if len(allTopics) > 0 {
		starts, startErr = f.admin.ListStartOffsets(ctx, allTopics.Topics()...)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("list start offsets: %w", err)
		}
		ends, endErr = f.admin.ListEndOffsets(ctx, allTopics.Topics()...)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("list end offsets: %w", err)
		}
	}
	result := make([]groups.ConsumerGroupSnapshot, 0, len(data))
	for _, group := range data {
		result = append(result, completeConsumerGroupSnapshot(group, starts, ends, startErr, endErr))
	}
	return result, nil
}
