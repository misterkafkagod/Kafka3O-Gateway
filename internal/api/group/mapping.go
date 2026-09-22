package group

import "github.com/misterkafkagod/kafka3o/internal/service/group"

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
