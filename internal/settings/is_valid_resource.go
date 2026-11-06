package settings

import (
	"strconv"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func IsValidResource(resource Resource) bool {
	switch resource.Kind {
	case Topic:
		return topics.IsValidTopicName(resource.Name)
	case Broker:
		id, err := strconv.ParseInt(resource.Name, 10, 32)
		return err == nil && id >= 0 && strconv.FormatInt(id, 10) == resource.Name
	default:
		return false
	}
}
