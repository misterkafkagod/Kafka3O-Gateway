package cluster

import (
	"net/http"
	"strconv"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
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

// toQuorumBody converts the port's QuorumStatus into the wire shape.
func toQuorumBody(q kafka.QuorumStatus) QuorumBody {
	return QuorumBody{
		LeaderID: q.LeaderID, Epoch: q.Epoch,
		Voters: toQuorumReplicaStateDTOs(q.Voters), Observers: toQuorumReplicaStateDTOs(q.Observers),
	}
}

func toQuorumReplicaStateDTOs(states []kafka.QuorumReplicaState) []QuorumReplicaStateDTO {
	out := make([]QuorumReplicaStateDTO, len(states))
	for i, s := range states {
		out[i] = QuorumReplicaStateDTO{ID: s.ID, LogEndOffset: s.LogEndOffset, LagMs: s.LagMs}
	}
	return out
}

// toReassignmentDTOs converts the port's PartitionReassignment list into
// the wire shape.
func toReassignmentDTOs(reassignments []kafka.PartitionReassignment) []ReassignmentDTO {
	out := make([]ReassignmentDTO, len(reassignments))
	for i, r := range reassignments {
		out[i] = ReassignmentDTO{
			Topic: r.Topic, Partition: r.Partition,
			Replicas: r.Replicas, AddingReplicas: r.AddingReplicas, RemovingReplicas: r.RemovingReplicas,
		}
	}
	return out
}

// toBrokerLogDirDTOs converts the port's BrokerLogDir list into the wire shape.
func toBrokerLogDirDTOs(dirs []kafka.BrokerLogDir) []BrokerLogDirDTO {
	out := make([]BrokerLogDirDTO, len(dirs))
	for i, d := range dirs {
		out[i] = BrokerLogDirDTO{BrokerID: d.BrokerID, LogDir: d.LogDir, TotalBytes: d.TotalBytes, Partitions: d.PartitionCount}
	}
	return out
}

// toOffsetsDTO converts a partition-keyed offset map into the wire shape
// (partition numbers as decimal-string JSON keys, matching every other
// partition-keyed map in this API).
func toOffsetsDTO(offsets map[int32]int64) map[string]int64 {
	out := make(map[string]int64, len(offsets))
	for p, offset := range offsets {
		out[strconv.Itoa(int(p))] = offset
	}
	return out
}

// toThroughputBody converts the service's ThroughputItem list into the wire shape.
func toThroughputBody(items []cluster.ThroughputItem, seconds int) ThroughputBody {
	out := make([]ThroughputItemDTO, len(items))
	for i, it := range items {
		out[i] = ThroughputItemDTO{
			Topic: it.Topic, MessagesPerSecond: it.MessagesPerSecond,
			StartEndOffsets: toOffsetsDTO(it.StartEndOffsets), FinishEndOffsets: toOffsetsDTO(it.FinishEndOffsets),
		}
	}
	return ThroughputBody{Seconds: seconds, Items: out}
}

// toExportTopicDTOs converts the service's ExportTopic list into the wire shape.
func toExportTopicDTOs(topics []cluster.ExportTopic) []ExportTopicDTO {
	out := make([]ExportTopicDTO, len(topics))
	for i, t := range topics {
		out[i] = ExportTopicDTO{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor, Configs: t.Configs}
	}
	return out
}

// toExportBody converts the service's Export into the wire shape.
func toExportBody(e cluster.Export) ExportBody {
	return ExportBody{ExportedAt: e.ExportedAt.UTC().Format(time.RFC3339Nano), Topics: toExportTopicDTOs(e.Topics)}
}

// toImportTopics converts the request DTO's topics into the service's
// ImportTopic (the same shape C11's export produces).
func toImportTopics(topics []ExportTopicDTO) []cluster.ImportTopic {
	out := make([]cluster.ImportTopic, len(topics))
	for i, t := range topics {
		out[i] = cluster.ImportTopic{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor, Configs: t.Configs}
	}
	return out
}

// toConfigChangeDetailDTOs converts the service's ConfigChangeDetail list
// into the wire shape.
func toConfigChangeDetailDTOs(changes []cluster.ConfigChangeDetail) []ClusterConfigChangeDetailDTO {
	out := make([]ClusterConfigChangeDetailDTO, len(changes))
	for i, c := range changes {
		out[i] = ClusterConfigChangeDetailDTO{Name: c.Name, From: c.From, To: c.To}
	}
	return out
}

// toAlterBrokerConfigPlanDTO converts the service's AlterBrokerConfigPlan
// into the wire shape.
func toAlterBrokerConfigPlanDTO(p cluster.AlterBrokerConfigPlan) *AlterBrokerConfigPlanDTO {
	return &AlterBrokerConfigPlanDTO{BrokerID: p.BrokerID, Changes: toConfigChangeDetailDTOs(p.Changes)}
}

// toBulkItemResultDTOs converts the service's core.BulkResult items into
// the wire shape, and reports the HTTP status: 200 when every item
// succeeded, 207 when the outcome is mixed (FUNC-SPEC §8.3 Bulk).
func toBulkItemResultDTOs(items []core.BulkItemResult) []BulkItemResultDTO {
	out := make([]BulkItemResultDTO, len(items))
	for i, item := range items {
		status := "ok"
		if item.Outcome != audit.OutcomeSucceeded {
			status = "failed"
		}
		out[i] = BulkItemResultDTO{Index: item.Index, Status: status, Error: item.Error}
	}
	return out
}

func bulkStatus(failed int) int {
	if failed > 0 {
		return http.StatusMultiStatus
	}
	return http.StatusOK
}

func toBulkSummaryDTO(s core.BulkSummary) *ClusterBulkSummaryDTO {
	return &ClusterBulkSummaryDTO{Total: s.Total, OK: s.Succeeded, Failed: s.Failed}
}

// toReassignMoveDTOs converts the service's ReassignMove list into the wire shape.
func toReassignMoveDTOs(moves []cluster.ReassignMove) []ReassignMoveDTO {
	out := make([]ReassignMoveDTO, len(moves))
	for i, m := range moves {
		out[i] = ReassignMoveDTO{Topic: m.Topic, Partition: m.Partition, Replicas: m.Replicas}
	}
	return out
}

// toReassignMoves converts the request DTO's reassignments into the
// service's ReassignMove.
func toReassignMoves(moves []ReassignMoveDTO) []cluster.ReassignMove {
	out := make([]cluster.ReassignMove, len(moves))
	for i, m := range moves {
		out[i] = cluster.ReassignMove{Topic: m.Topic, Partition: m.Partition, Replicas: m.Replicas}
	}
	return out
}

// toReassignPlanDTO converts the service's ReassignPlan into the wire shape.
func toReassignPlanDTO(p cluster.ReassignPlan) *ReassignPlanDTO {
	return &ReassignPlanDTO{Moves: toReassignMoveDTOs(p.Moves), PlanToken: p.PlanToken}
}

// toCancelTargets converts the request DTO's cancel list into the port's
// TopicPartition.
func toCancelTargets(targets []CancelReassignmentTargetDTO) []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, len(targets))
	for i, t := range targets {
		out[i] = kafka.TopicPartition{Topic: t.Topic, Partition: t.Partition}
	}
	return out
}

// toElectTargets converts the request DTO's elect partitions into the
// port's TopicPartition.
func toElectTargets(targets []ElectTargetDTO) []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, len(targets))
	for i, t := range targets {
		out[i] = kafka.TopicPartition{Topic: t.Topic, Partition: t.Partition}
	}
	return out
}

// toElectPlanDTO converts the service's ElectPlan into the wire shape.
func toElectPlanDTO(p cluster.ElectPlan) *ElectPlanDTO {
	elections := make([]ElectTargetDTO, len(p.Partitions))
	for i, tp := range p.Partitions {
		elections[i] = ElectTargetDTO{Topic: tp.Topic, Partition: tp.Partition}
	}
	return &ElectPlanDTO{Elections: elections, PlanToken: p.PlanToken}
}

// toImportAlterActionDTOs converts the service's ImportAlterAction list
// into the wire shape.
func toImportAlterActionDTOs(actions []cluster.ImportAlterAction) []ImportAlterActionDTO {
	out := make([]ImportAlterActionDTO, len(actions))
	for i, a := range actions {
		out[i] = ImportAlterActionDTO{Name: a.Name, Changes: toConfigChangeDetailDTOs(a.Changes)}
	}
	return out
}

// toImportPlanDTO converts the service's ImportPlan into the wire shape.
func toImportPlanDTO(p cluster.ImportPlan) *ApplyTopicsPlanDTO {
	return &ApplyTopicsPlanDTO{
		Create: toImportTopicDTOs(p.Create),
		Alter:  toImportAlterActionDTOs(p.Alter),
		Delete: p.Delete, Unchanged: p.Unchanged, PlanToken: p.PlanToken,
	}
}

// toImportTopicDTOs converts the service's ImportTopic list into the wire
// shape — the same ExportTopicDTO shape C11's export and C12's own request
// body both use (ImportTopic and ExportTopic share the same fields).
func toImportTopicDTOs(topics []cluster.ImportTopic) []ExportTopicDTO {
	out := make([]ExportTopicDTO, len(topics))
	for i, t := range topics {
		out[i] = ExportTopicDTO{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor, Configs: t.Configs}
	}
	return out
}

// toImportResultBody converts the service's ImportResult into the wire
// shape, and reports the HTTP status: 200 when every attempted item
// succeeded (skipped items don't count as failures), 207 when mixed.
func toImportResultBody(r cluster.ImportResult) (ApplyTopicsBody, int) {
	items := make([]ApplyTopicsItemResultDTO, len(r.Items))
	for i, item := range r.Items {
		status := "ok"
		switch {
		case item.Action == "skipped":
			status = "skipped"
		case item.Outcome != audit.OutcomeSucceeded:
			status = "failed"
		}
		items[i] = ApplyTopicsItemResultDTO{Name: item.Name, Action: item.Action, Status: status, Error: item.Error}
	}
	return ApplyTopicsBody{Items: items, Summary: toBulkSummaryDTO(r.Summary)}, bulkStatus(r.Summary.Failed)
}
