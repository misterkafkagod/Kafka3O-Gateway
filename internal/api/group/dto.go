// Package group wires the consumer-group inspection commands (FUNC-SPEC
// §8.7 G1, G2) onto Huma operations: routes, DTOs, and the mapping between
// them and internal/service/group's plain domain results (TECH-SPEC I5).
// G3 (topic -> consumer groups) is registered under internal/api/topic
// instead (TECH-SPEC §6.2: it lives on the topics path), even though its
// service method is group.Service.ConsumersOfTopic.
package group

// PageBounds is the configured default and ceiling for ?pageSize= (FUNC-SPEC
// §8.8 pageSize row). Page itself is always 1-based with no configurable
// ceiling.
type PageBounds struct {
	Default int
	Ceiling int
}

// GroupSummaryDTO is one group in a List response (FUNC-SPEC §8.7 G1).
type GroupSummaryDTO struct {
	GroupID      string `json:"groupId"`
	State        string `json:"state"`
	ProtocolType string `json:"protocolType"`
	MemberCount  int    `json:"memberCount"`
}

// GroupPageDTO is the pagination envelope every list response carries (FUNC-SPEC
// §8.2 `?page`/`?pageSize`).
type GroupPageDTO struct {
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`
	Total    int `json:"total"`
}

// ListGroupsInput is GET /v1/consumer-groups's parameters (FUNC-SPEC §8.7
// G1; TECH-SPEC §6.2). PageSize has no static maximum tag: its ceiling is
// configured (FUNC-SPEC §8.8), enforced in the handler against PageBounds.
type ListGroupsInput struct {
	State    string `query:"state"`
	Page     int    `query:"page" default:"1" minimum:"1"`
	PageSize int    `query:"pageSize"`
}

// ListGroupsBody is GET /v1/consumer-groups's response body (FUNC-SPEC §8.3 List).
type ListGroupsBody struct {
	Items []GroupSummaryDTO `json:"items"`
	Page  GroupPageDTO      `json:"page"`
}

// ListGroupsOutput wraps ListGroupsBody for Huma.
type ListGroupsOutput struct {
	Body ListGroupsBody
}

// DescribeGroupInput identifies the group to describe (FUNC-SPEC §8.7 G2).
type DescribeGroupInput struct {
	GroupID string `path:"groupId"`
}

// MemberAssignmentDTO is one topic-partition a member is assigned
// (FUNC-SPEC §8.7 G2).
type MemberAssignmentDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// GroupMemberDTO is one live member of a group (FUNC-SPEC §8.7 G2).
type GroupMemberDTO struct {
	MemberID    string                `json:"memberId"`
	ClientID    string                `json:"clientId"`
	Host        string                `json:"host"`
	Assignments []MemberAssignmentDTO `json:"assignments"`
}

// GroupOffsetDTO is one partition's committed offset, end offset, and lag
// (FUNC-SPEC §8.7 G2).
type GroupOffsetDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"`
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
}

// DescribeGroupBody is GET /v1/consumer-groups/{groupId}'s response body
// (FUNC-SPEC §8.7 G2).
type DescribeGroupBody struct {
	GroupID       string           `json:"groupId"`
	State         string           `json:"state"`
	CoordinatorID int32            `json:"coordinatorId"`
	Members       []GroupMemberDTO `json:"members"`
	Offsets       []GroupOffsetDTO `json:"offsets"`
	TotalLag      int64            `json:"totalLag"`
}

// DescribeGroupOutput wraps DescribeGroupBody for Huma.
type DescribeGroupOutput struct {
	Body DescribeGroupBody
}
