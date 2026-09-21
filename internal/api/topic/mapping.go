package topic

import (
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

// toTopicConfigEntryDTOs converts ConfigEntry values into the wire shape, nulling
// a sensitive entry's value (FUNC-SPEC §8.2).
func toTopicConfigEntryDTOs(configs []kafka.ConfigEntry) []TopicConfigEntryDTO {
	out := make([]TopicConfigEntryDTO, len(configs))
	for i, c := range configs {
		dto := TopicConfigEntryDTO{
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

// toListItems converts the service's ListItem slice into the wire shape.
func toListItems(items []topic.ListItem) []TopicSummaryDTO {
	out := make([]TopicSummaryDTO, len(items))
	for i, it := range items {
		out[i] = TopicSummaryDTO{
			Name:              it.Name,
			Partitions:        it.PartitionCount,
			ReplicationFactor: it.ReplicationFactor,
			Internal:          it.Internal,
		}
	}
	return out
}

// toDescribeBody converts the service's Describe result into the wire shape.
func toDescribeBody(d topic.Describe) DescribeTopicBody {
	partitions := make([]PartitionDetailDTO, len(d.Partitions))
	for i, p := range d.Partitions {
		partitions[i] = PartitionDetailDTO{
			ID:          p.ID,
			Leader:      p.Leader,
			Replicas:    p.Replicas,
			ISR:         p.ISR,
			BeginOffset: p.BeginOffset,
			EndOffset:   p.EndOffset,
			ApproxCount: p.ApproxCount,
		}
	}
	return DescribeTopicBody{
		Name:               d.Name,
		Internal:           d.Internal,
		PartitionCount:     len(d.Partitions),
		ReplicationFactor:  d.ReplicationFactor,
		ApproxMessageCount: d.ApproxMessageCount,
		Partitions:         partitions,
		Configs:            toTopicConfigEntryDTOs(d.Configs),
	}
}

// toSizeBody converts the service's Size result into the wire shape.
func toSizeBody(s topic.Size) TopicSizeBody {
	partitions := make([]PartitionSizeDTO, len(s.Partitions))
	for i, p := range s.Partitions {
		replicas := make([]ReplicaSizeDTO, len(p.Replicas))
		for j, r := range p.Replicas {
			replicas[j] = ReplicaSizeDTO{BrokerID: r.BrokerID, LogDir: r.LogDir, Bytes: r.Bytes}
		}
		partitions[i] = PartitionSizeDTO{ID: p.ID, Bytes: p.Bytes, Replicas: replicas}
	}
	return TopicSizeBody{Topic: s.Topic, TotalBytes: s.TotalBytes, Partitions: partitions}
}

// toCountBody converts the service's Count result into the wire shape. from
// and to are the original request strings, echoed back verbatim (FUNC-SPEC
// §8.7 T4).
func toCountBody(c topic.Count, from, to string) TopicCountBody {
	partitions := make([]PartitionCountDTO, len(c.Partitions))
	for i, p := range c.Partitions {
		partitions[i] = PartitionCountDTO{ID: p.ID, FromOffset: p.FromOffset, ToOffset: p.ToOffset, Count: p.Count}
	}
	return TopicCountBody{Topic: c.Topic, From: from, To: to, Total: c.Total, Partitions: partitions}
}
