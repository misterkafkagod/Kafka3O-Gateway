package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type groupMembershipAdmin interface {
	ListGroupsByType(context.Context, []string, ...string) (kadm.ListedGroups, error)
	DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error)
	DescribeConsumerGroups(context.Context, ...string) (kadm.DescribedConsumerGroups, error)
}

// GroupInactiveCheck confirms that a consumer group exists and has no members.
type GroupInactiveCheck interface {
	Execute(ctx context.Context, group string) error
}

type ensureGroupInactive struct {
	admin groupMembershipAdmin
}

func NewEnsureGroupInactive(admin groupMembershipAdmin) GroupInactiveCheck {
	return ensureGroupInactive{admin: admin}
}

// Execute returns groups.ErrGroupNotFound for unknown or non-consumer groups and groups.ErrGroupActive when the
// group has members; offsets may only be reset while nobody consumes.
func (e ensureGroupInactive) Execute(ctx context.Context, group string) error {
	classic, err := e.admin.ListGroupsByType(ctx, []string{"classic"})
	if err != nil {
		return fmt.Errorf("list classic groups: %w", err)
	}
	modern, err := e.admin.ListGroupsByType(ctx, []string{"consumer"})
	if err != nil {
		return fmt.Errorf("list consumer groups: %w", err)
	}
	if listed, ok := classic[group]; ok && (listed.ProtocolType == "" || listed.ProtocolType == "consumer") {
		described, err := e.admin.DescribeGroups(ctx, group)
		entry, ok := described[group]
		if !ok || entry.Err != nil {
			return fmt.Errorf("describe group: %w", errors.Join(entry.Err, err, errors.New("no description")))
		}
		if len(entry.Members) > 0 {
			return groups.ErrGroupActive
		}
		return nil
	}
	if _, ok := modern[group]; ok {
		described, err := e.admin.DescribeConsumerGroups(ctx, group)
		entry, ok := described[group]
		if !ok || entry.Err != nil {
			return fmt.Errorf("describe consumer group: %w", errors.Join(entry.Err, err, errors.New("no description")))
		}
		if len(entry.Members) > 0 {
			return groups.ErrGroupActive
		}
		return nil
	}
	return groups.ErrGroupNotFound
}
