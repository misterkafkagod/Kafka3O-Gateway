package group

import (
	"strconv"
	"strings"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
)

// toGroupSummaryDTOs converts the service's List items into the wire shape.
func toGroupSummaryDTOs(items []group.ListItem) []GroupSummaryDTO {
	out := make([]GroupSummaryDTO, len(items))
	for i, it := range items {
		out[i] = GroupSummaryDTO{
			GroupID: it.ID, State: it.State, ProtocolType: it.ProtocolType, MemberCount: it.MemberCount,
		}
	}
	return out
}

// toDescribeGroupBody converts the service's Describe result into the wire shape.
func toDescribeGroupBody(d group.Describe) DescribeGroupBody {
	members := make([]GroupMemberDTO, len(d.Members))
	for i, m := range d.Members {
		assignments := make([]MemberAssignmentDTO, len(m.Assignments))
		for j, a := range m.Assignments {
			assignments[j] = MemberAssignmentDTO{Topic: a.Topic, Partition: a.Partition}
		}
		members[i] = GroupMemberDTO{
			MemberID: m.MemberID, ClientID: m.ClientID, Host: m.Host, Assignments: assignments,
		}
	}
	offsets := make([]GroupOffsetDTO, len(d.Offsets))
	for i, o := range d.Offsets {
		offsets[i] = GroupOffsetDTO{
			Topic: o.Topic, Partition: o.Partition, Committed: o.Committed, End: o.End, Lag: o.Lag,
		}
	}
	return DescribeGroupBody{
		GroupID: d.ID, State: d.State, CoordinatorID: d.CoordinatorID,
		Members: members, Offsets: offsets, TotalLag: d.TotalLag,
	}
}

// toResetTarget converts the request DTO into the service's ResetTarget,
// parsing each Offsets key ("<topic>:<partition>") back into a
// kafka.TopicPartition — Kafka topic names never contain ':', so splitting
// on the last one is unambiguous. A malformed key → 400 core.Validation.
func toResetTarget(b ResetOffsetsTargetDTO) (group.ResetTarget, error) {
	target := group.ResetTarget{Mode: b.Mode, Offset: b.Offset, TimestampMs: b.TimestampMs}
	if len(b.Offsets) == 0 {
		return target, nil
	}
	target.Offsets = make(map[kafka.TopicPartition]int64, len(b.Offsets))
	for key, at := range b.Offsets {
		i := strings.LastIndex(key, ":")
		if i < 0 {
			return group.ResetTarget{}, &core.PolicyError{Code: core.Validation, Message: "target.offsets key " + key + " must be \"<topic>:<partition>\""}
		}
		partition, err := strconv.ParseInt(key[i+1:], 10, 32)
		if err != nil {
			return group.ResetTarget{}, &core.PolicyError{Code: core.Validation, Message: "target.offsets key " + key + " must be \"<topic>:<partition>\""}
		}
		target.Offsets[kafka.TopicPartition{Topic: key[:i], Partition: int32(partition)}] = at
	}
	return target, nil
}

// toResetOffsetsPlanDTO converts the service's ResetPlan into the wire
// shape (dry-run: current/target).
func toResetOffsetsPlanDTO(p group.ResetPlan) *ResetOffsetsPlanDTO {
	offsets := make([]ResetOffsetPlanDetailDTO, len(p.Offsets))
	for i, d := range p.Offsets {
		offsets[i] = ResetOffsetPlanDetailDTO{Topic: d.Topic, Partition: d.Partition, Current: d.Before, Target: d.After}
	}
	return &ResetOffsetsPlanDTO{Offsets: offsets}
}

// toResetOffsetResultDTOs converts the service's ResetOffsetDetail slice
// into the wire shape (executed: before/after).
func toResetOffsetResultDTOs(details []group.ResetOffsetDetail) []ResetOffsetResultDetailDTO {
	out := make([]ResetOffsetResultDetailDTO, len(details))
	for i, d := range details {
		out[i] = ResetOffsetResultDetailDTO{Topic: d.Topic, Partition: d.Partition, Before: d.Before, After: d.After}
	}
	return out
}

// toDeleteGroupPlanDTO converts the service's DeleteGroupPlan into the wire
// shape.
func toDeleteGroupPlanDTO(p group.DeleteGroupPlan) *DeleteGroupPlanDTO {
	return &DeleteGroupPlanDTO{GroupID: p.GroupID, CommittedOffsets: p.CommittedOffsets}
}

// toRemoveMembersPlanDTO converts the service's RemoveMembersPlan into the
// wire shape.
func toRemoveMembersPlanDTO(p group.RemoveMembersPlan) *RemoveMembersPlanDTO {
	return &RemoveMembersPlanDTO{Members: p.Members}
}

// toCloneOffsetsPlanDTO converts the service's CloneOffsetsPlan into the
// wire shape.
func toCloneOffsetsPlanDTO(p group.CloneOffsetsPlan) *CloneOffsetsPlanDTO {
	offsets := make([]CloneOffsetPlanDetailDTO, len(p.Offsets))
	for i, d := range p.Offsets {
		offsets[i] = CloneOffsetPlanDetailDTO{Topic: d.Topic, Partition: d.Partition, TargetCurrent: d.TargetCurrent, NewValue: d.NewValue}
	}
	return &CloneOffsetsPlanDTO{Offsets: offsets}
}

// toCloneOffsetResultDTOs converts the service's CloneOffsetDetail slice
// into the wire shape (executed: offset).
func toCloneOffsetResultDTOs(details []group.CloneOffsetDetail) []CloneOffsetResultDetailDTO {
	out := make([]CloneOffsetResultDetailDTO, len(details))
	for i, d := range details {
		out[i] = CloneOffsetResultDetailDTO{Topic: d.Topic, Partition: d.Partition, Offset: d.NewValue}
	}
	return out
}
