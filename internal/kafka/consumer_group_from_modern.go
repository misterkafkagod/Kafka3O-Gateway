package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

func consumerGroupFromModern(listed kadm.ListedGroup, described *kadm.DescribedConsumerGroup, describeErr error) consumerGroupData {
	data := consumerGroupData{
		snapshot: groups.ConsumerGroupSnapshot{ID: listed.Group, Protocol: "consumer", State: listed.State},
		topics:   make(kadm.TopicsSet),
	}
	if described == nil {
		if describeErr != nil {
			data.problems = append(data.problems, fmt.Sprintf("describe group: %v", describeErr))
		} else {
			data.problems = append(data.problems, "describe group: missing response")
		}
		return data
	}
	if described.Err != nil {
		data.problems = append(data.problems, fmt.Sprintf("describe group: %v", described.Err))
		return data
	}
	data.snapshot.State = described.State
	for _, member := range described.Members {
		data.snapshot.Members = append(data.snapshot.Members, groups.ConsumerGroupMember{
			ID: member.MemberID, ClientID: member.ClientID, ClientHost: member.ClientHost, InstanceID: member.InstanceID,
		})
		data.topics.Merge(member.Assignment)
	}
	data.topics.MergeTopics(described.SubscribedTopics())
	return data
}
