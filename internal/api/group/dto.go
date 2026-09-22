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

// ResetOffsetsTargetDTO is G4's `target` request field (FUNC-SPEC §8.7 G4).
// Offsets, when non-empty, overrides Mode/Offset/TimestampMs entirely; its
// keys are "<topic>:<partition>" (Kafka topic names never contain ':').
type ResetOffsetsTargetDTO struct {
	Mode        string           `json:"mode"`
	Offset      int64            `json:"offset,omitempty"`
	TimestampMs int64            `json:"timestampMs,omitempty"`
	Offsets     map[string]int64 `json:"offsets,omitempty"`
}

// ResetOffsetsRequestBody is POST .../reset-offsets's request body
// (FUNC-SPEC §8.7 G4).
type ResetOffsetsRequestBody struct {
	Confirm string                `json:"confirm"`
	Target  ResetOffsetsTargetDTO `json:"target"`
	Topics  []string              `json:"topics,omitempty"`
}

// ResetOffsetsInput is POST .../reset-offsets's parameters (FUNC-SPEC §8.7
// G4, §8.2 `?dryRun=true`).
type ResetOffsetsInput struct {
	GroupID string `path:"groupId"`
	DryRun  bool   `query:"dryRun"`
	Body    ResetOffsetsRequestBody
}

// ResetOffsetPlanDetailDTO is one partition's planned move (FUNC-SPEC §8.6 G4).
type ResetOffsetPlanDetailDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Current   int64  `json:"current"`
	Target    int64  `json:"target"`
}

// ResetOffsetsPlanDTO is a dry-run G4's plan (FUNC-SPEC §8.6).
type ResetOffsetsPlanDTO struct {
	Offsets []ResetOffsetPlanDetailDTO `json:"offsets"`
}

// ResetOffsetResultDetailDTO is one partition's applied move (FUNC-SPEC §8.7 G4).
type ResetOffsetResultDetailDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Before    int64  `json:"before"`
	After     int64  `json:"after"`
}

// ResetOffsetsBody is POST .../reset-offsets's response body: either the
// applied offset moves directly (FUNC-SPEC §8.7 G4), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope. See topic.CreateTopicBody for why both
// shapes share one Go type.
type ResetOffsetsBody struct {
	DryRun  bool                         `json:"dryRun,omitempty"`
	Plan    *ResetOffsetsPlanDTO         `json:"plan,omitempty"`
	GroupID string                       `json:"groupId,omitempty"`
	Offsets []ResetOffsetResultDetailDTO `json:"offsets,omitempty"`
}

// ResetOffsetsOutput wraps ResetOffsetsBody for Huma.
type ResetOffsetsOutput struct {
	Body ResetOffsetsBody
}

// DeleteGroupRequestBody is DELETE /v1/consumer-groups/{groupId}'s request
// body (FUNC-SPEC §8.7 G5). Carried in the DELETE body itself (TECH-SPEC
// R3, matching topic.DeleteTopicRequestBody's own note).
type DeleteGroupRequestBody struct {
	Confirm string `json:"confirm"`
}

// DeleteGroupInput is DELETE /v1/consumer-groups/{groupId}'s parameters
// (FUNC-SPEC §8.7 G5, §8.2 `?dryRun=true`).
type DeleteGroupInput struct {
	GroupID string `path:"groupId"`
	DryRun  bool   `query:"dryRun"`
	Body    DeleteGroupRequestBody
}

// DeleteGroupPlanDTO is a dry-run G5's plan (FUNC-SPEC §8.6).
type DeleteGroupPlanDTO struct {
	GroupID          string `json:"groupId"`
	CommittedOffsets int    `json:"committedOffsets"`
}

// DeleteGroupBody is DELETE /v1/consumer-groups/{groupId}'s response body:
// either the deleted group's id directly (FUNC-SPEC §8.7 G5), or — when
// DryRun — the FUNC-SPEC §8.3 dry-run envelope.
type DeleteGroupBody struct {
	DryRun  bool                `json:"dryRun,omitempty"`
	Plan    *DeleteGroupPlanDTO `json:"plan,omitempty"`
	Deleted string              `json:"deleted,omitempty"`
}

// DeleteGroupOutput wraps DeleteGroupBody for Huma.
type DeleteGroupOutput struct {
	Body DeleteGroupBody
}

// RemoveMembersRequestBody is POST .../remove-members's request body
// (FUNC-SPEC §8.7 G6). Members omitted or empty means every current member.
type RemoveMembersRequestBody struct {
	Confirm string   `json:"confirm"`
	Members []string `json:"members,omitempty"`
}

// RemoveMembersInput is POST .../remove-members's parameters (FUNC-SPEC §8.7
// G6, §8.2 `?dryRun=true`).
type RemoveMembersInput struct {
	GroupID string `path:"groupId"`
	DryRun  bool   `query:"dryRun"`
	Body    RemoveMembersRequestBody
}

// RemoveMembersPlanDTO is a dry-run G6's plan (FUNC-SPEC §8.6).
type RemoveMembersPlanDTO struct {
	Members []string `json:"members"`
}

// RemoveMembersBody is POST .../remove-members's response body: either the
// removed member ids directly (FUNC-SPEC §8.7 G6), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope.
type RemoveMembersBody struct {
	DryRun  bool                  `json:"dryRun,omitempty"`
	Plan    *RemoveMembersPlanDTO `json:"plan,omitempty"`
	Removed []string              `json:"removed,omitempty"`
}

// RemoveMembersOutput wraps RemoveMembersBody for Huma.
type RemoveMembersOutput struct {
	Body RemoveMembersBody
}

// CloneOffsetsRequestBody is POST /v1/consumer-groups/{target}/clone-offsets's
// request body (FUNC-SPEC §8.7 G7).
type CloneOffsetsRequestBody struct {
	Confirm string   `json:"confirm"`
	Source  string   `json:"source"`
	Topics  []string `json:"topics,omitempty"`
}

// CloneOffsetsInput is POST .../clone-offsets's parameters (FUNC-SPEC §8.7
// G7 target in path, §8.2 `?dryRun=true`).
type CloneOffsetsInput struct {
	Target string `path:"target"`
	DryRun bool   `query:"dryRun"`
	Body   CloneOffsetsRequestBody
}

// CloneOffsetPlanDetailDTO is one partition's planned clone (FUNC-SPEC §8.6 G7).
type CloneOffsetPlanDetailDTO struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	TargetCurrent int64  `json:"targetCurrent"`
	NewValue      int64  `json:"newValue"`
}

// CloneOffsetsPlanDTO is a dry-run G7's plan (FUNC-SPEC §8.6).
type CloneOffsetsPlanDTO struct {
	Offsets []CloneOffsetPlanDetailDTO `json:"offsets"`
}

// CloneOffsetResultDetailDTO is one partition's applied clone (FUNC-SPEC §8.7 G7).
type CloneOffsetResultDetailDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// CloneOffsetsBody is POST .../clone-offsets's response body: either the
// applied clone directly (FUNC-SPEC §8.7 G7), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope.
type CloneOffsetsBody struct {
	DryRun  bool                         `json:"dryRun,omitempty"`
	Plan    *CloneOffsetsPlanDTO         `json:"plan,omitempty"`
	Target  string                       `json:"target,omitempty"`
	Offsets []CloneOffsetResultDetailDTO `json:"offsets,omitempty"`
}

// CloneOffsetsOutput wraps CloneOffsetsBody for Huma.
type CloneOffsetsOutput struct {
	Body CloneOffsetsBody
}
