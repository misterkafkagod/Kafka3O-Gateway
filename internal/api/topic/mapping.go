package topic

import (
	"net/http"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
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

// toTopicConsumerGroupsBody converts the group service's reverse-lookup
// result into the wire shape (FUNC-SPEC §8.7 G3).
func toTopicConsumerGroupsBody(topicName string, groups []group.TopicGroup) TopicConsumerGroupsBody {
	out := make([]TopicConsumerGroupDTO, len(groups))
	for i, g := range groups {
		partitions := make([]GroupPartitionLagDTO, len(g.Partitions))
		for j, p := range g.Partitions {
			partitions[j] = GroupPartitionLagDTO{Partition: p.Partition, Committed: p.Committed, End: p.End, Lag: p.Lag}
		}
		out[i] = TopicConsumerGroupDTO{GroupID: g.GroupID, State: g.State, TotalLag: g.TotalLag, Partitions: partitions}
	}
	return TopicConsumerGroupsBody{Topic: topicName, Groups: out}
}

// toCreateParams converts one request body into the service's params.
func toCreateParams(b CreateTopicRequestBody) topic.CreateParams {
	return topic.CreateParams{Name: b.Name, Partitions: b.Partitions, ReplicationFactor: b.ReplicationFactor, Configs: b.Configs}
}

// toCreateTopicPlanDTO converts the service's CreateResult into the plan
// shape a dry-run T5 (or T6 item) returns.
func toCreateTopicPlanDTO(r topic.CreateResult) *CreateTopicPlanDTO {
	return &CreateTopicPlanDTO{
		Name: r.Name, Partitions: r.Partitions, ReplicationFactor: r.ReplicationFactor,
		Configs: toTopicConfigEntryDTOs(r.Configs),
	}
}

// toCreateTopicsBulkBody converts the service's BulkResult (plus the
// original per-item names, since core.BulkItemResult carries none) into
// the wire bulk envelope (FUNC-SPEC §8.3 Bulk), and reports the HTTP
// status: 200 when every item succeeded, 207 when the outcome is mixed.
func toCreateTopicsBulkBody(topics []CreateTopicRequestBody, r core.BulkResult) (CreateTopicsBulkBody, int) {
	items := make([]CreateBulkItemResultDTO, len(r.Items))
	for i, item := range r.Items {
		status := "ok"
		if item.Outcome != audit.OutcomeSucceeded {
			status = "failed"
		}
		name := ""
		if item.Index < len(topics) {
			name = topics[item.Index].Name
		}
		items[i] = CreateBulkItemResultDTO{Index: item.Index, Status: status, Name: name, Error: item.Error}
	}
	status := http.StatusOK
	if r.Summary.Failed > 0 {
		status = http.StatusMultiStatus
	}
	return CreateTopicsBulkBody{
		Items:   items,
		Summary: TopicBulkSummaryDTO{Total: r.Summary.Total, OK: r.Summary.Succeeded, Failed: r.Summary.Failed},
	}, status
}

// toAlterConfigPlanDTO converts the service's AlterConfigPlan into the wire
// shape.
func toAlterConfigPlanDTO(p topic.AlterConfigPlan) *AlterConfigPlanDTO {
	changes := make([]ConfigChangeDetailDTO, len(p.Changes))
	for i, c := range p.Changes {
		changes[i] = ConfigChangeDetailDTO{Name: c.Name, From: c.From, To: c.To}
	}
	return &AlterConfigPlanDTO{Topic: p.Topic, Changes: changes}
}

// toAddPartitionsPlanDTO converts the service's AddPartitionsPlan into the
// wire shape.
func toAddPartitionsPlanDTO(p topic.AddPartitionsPlan) *AddPartitionsPlanDTO {
	return &AddPartitionsPlanDTO{Topic: p.Topic, From: p.From, To: p.To, Warning: p.Warning}
}
