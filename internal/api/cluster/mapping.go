package cluster

import (
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
)

// toBrokerDTOs converts the port's Broker list into the wire shape.
func toBrokerDTOs(brokers []kafka.Broker) []BrokerDTO {
	out := make([]BrokerDTO, len(brokers))
	for i, b := range brokers {
		out[i] = BrokerDTO{ID: b.ID, Host: b.Host, Port: b.Port, Rack: b.Rack}
	}
	return out
}

// toBrokerConfigEntryDTOs converts ConfigEntry values into the wire shape, nulling
// a sensitive entry's value (FUNC-SPEC §8.2).
func toBrokerConfigEntryDTOs(configs []kafka.ConfigEntry) []BrokerConfigEntryDTO {
	out := make([]BrokerConfigEntryDTO, len(configs))
	for i, c := range configs {
		dto := BrokerConfigEntryDTO{
			Name:        c.Name,
			Source:      string(c.Source),
			IsSensitive: c.IsSensitive,
			IsReadOnly:  c.IsReadOnly,
		}
		if !c.IsSensitive {
			v := c.Value
			dto.Value = &v
		}
		out[i] = dto
	}
	return out
}

// toHealthSummaryBody converts the service's HealthSummary into the wire
// shape (FUNC-SPEC §8.7 C4; TECH-SPEC C11).
func toHealthSummaryBody(h cluster.HealthSummary) ClusterHealthBody {
	affected := make([]AffectedDTO, len(h.Affected))
	for i, a := range h.Affected {
		affected[i] = AffectedDTO{Topic: a.Topic, Partition: a.Partition, Issue: a.Issue}
	}
	return ClusterHealthBody{
		Brokers: BrokersHealthDTO{Online: h.BrokersOnline},
		Topics:  TopicsHealthDTO{Total: h.TopicsTotal},
		Partitions: PartitionsHealthDTO{
			Total:              h.PartitionsTotal,
			UnderReplicated:    h.PartitionsUnderReplicated,
			Offline:            h.PartitionsOffline,
			NonPreferredLeader: h.PartitionsNonPreferredLeader,
		},
		Affected:  affected,
		Truncated: h.Truncated,
	}
}
