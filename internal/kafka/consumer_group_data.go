package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type consumerGroupData struct {
	snapshot    groups.ConsumerGroupSnapshot
	topics      kadm.TopicsSet
	commits     kadm.OffsetResponses
	commitError error
	problems    []string
}
