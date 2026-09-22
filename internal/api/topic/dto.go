// Package topic wires the topic-inspection commands (FUNC-SPEC §8.7 T1-T4)
// onto Huma operations: routes, DTOs, and the mapping between them and
// internal/service/topic's plain domain results (TECH-SPEC I5).
package topic

// PageBounds is the configured default and ceiling for ?pageSize= (FUNC-SPEC
// §8.8 pageSize row). Page itself is always 1-based with no configurable
// ceiling.
type PageBounds struct {
	Default int
	Ceiling int
}

// TopicConfigEntryDTO is one configuration property (FUNC-SPEC §8.7 T2). Value is
// nil (JSON null) for a sensitive entry (FUNC-SPEC §8.2).
type TopicConfigEntryDTO struct {
	Name        string  `json:"name"`
	Value       *string `json:"value"`
	Source      string  `json:"source"`
	IsSensitive bool    `json:"isSensitive"`
	IsReadOnly  bool    `json:"isReadOnly"`
}

// ListTopicsInput is GET /v1/topics's parameters (FUNC-SPEC §8.7 T1;
// TECH-SPEC §6.2). PageSize has no static maximum tag: its ceiling is
// configured (FUNC-SPEC §8.8), so it is enforced in the handler against
// PageBounds rather than in the schema.
type ListTopicsInput struct {
	Pattern         string `query:"pattern"`
	IncludeInternal bool   `query:"includeInternal"`
	Page            int    `query:"page" default:"1" minimum:"1"`
	PageSize        int    `query:"pageSize"`
}

// TopicSummaryDTO is one topic in a List response (FUNC-SPEC §8.7 T1).
type TopicSummaryDTO struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	Internal          bool   `json:"internal"`
}

// PageDTO is the pagination envelope every list response carries (FUNC-SPEC
// §8.2 `?page`/`?pageSize`).
type PageDTO struct {
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`
	Total    int `json:"total"`
}

// ListTopicsBody is GET /v1/topics's response body.
type ListTopicsBody struct {
	Items []TopicSummaryDTO `json:"items"`
	Page  PageDTO           `json:"page"`
}

// ListTopicsOutput wraps ListTopicsBody for Huma.
type ListTopicsOutput struct {
	Body ListTopicsBody
}

// DescribeTopicInput identifies the topic to describe (FUNC-SPEC §8.7 T2).
type DescribeTopicInput struct {
	Name string `path:"name"`
}

// PartitionDetailDTO is one partition in a Describe response (FUNC-SPEC §8.7 T2).
type PartitionDetailDTO struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	BeginOffset int64   `json:"beginOffset"`
	EndOffset   int64   `json:"endOffset"`
	ApproxCount int64   `json:"approxCount"`
}

// DescribeTopicBody is GET /v1/topics/{name}'s response body (FUNC-SPEC §8.7 T2).
type DescribeTopicBody struct {
	Name               string                `json:"name"`
	Internal           bool                  `json:"internal"`
	PartitionCount     int                   `json:"partitionCount"`
	ReplicationFactor  int                   `json:"replicationFactor"`
	ApproxMessageCount int64                 `json:"approxMessageCount"`
	Partitions         []PartitionDetailDTO  `json:"partitions"`
	Configs            []TopicConfigEntryDTO `json:"configs"`
}

// DescribeTopicOutput wraps DescribeTopicBody for Huma.
type DescribeTopicOutput struct {
	Body DescribeTopicBody
}

// TopicSizeInput identifies the topic to size (FUNC-SPEC §8.7 T3).
type TopicSizeInput struct {
	Name string `path:"name"`
}

// ReplicaSizeDTO is one replica's on-disk footprint (FUNC-SPEC §8.7 T3).
type ReplicaSizeDTO struct {
	BrokerID int32  `json:"brokerId"`
	LogDir   string `json:"logDir"`
	Bytes    int64  `json:"bytes"`
}

// PartitionSizeDTO is one partition's total size and per-replica breakdown
// (FUNC-SPEC §8.7 T3).
type PartitionSizeDTO struct {
	ID       int32            `json:"id"`
	Bytes    int64            `json:"bytes"`
	Replicas []ReplicaSizeDTO `json:"replicas"`
}

// TopicSizeBody is GET /v1/topics/{name}/size's response body.
type TopicSizeBody struct {
	Topic      string             `json:"topic"`
	TotalBytes int64              `json:"totalBytes"`
	Partitions []PartitionSizeDTO `json:"partitions"`
}

// TopicSizeOutput wraps TopicSizeBody for Huma.
type TopicSizeOutput struct {
	Body TopicSizeBody
}

// TopicCountInput is GET /v1/topics/{name}/count's parameters (FUNC-SPEC §8.7
// T4). From and To are RFC3339 timestamps.
type TopicCountInput struct {
	Name string `path:"name"`
	From string `query:"from" required:"true"`
	To   string `query:"to" required:"true"`
}

// PartitionCountDTO is one partition's count within the window (FUNC-SPEC
// §8.7 T4).
type PartitionCountDTO struct {
	ID         int32 `json:"id"`
	FromOffset int64 `json:"fromOffset"`
	ToOffset   int64 `json:"toOffset"`
	Count      int64 `json:"count"`
}

// TopicCountBody is GET /v1/topics/{name}/count's response body.
type TopicCountBody struct {
	Topic      string              `json:"topic"`
	From       string              `json:"from"`
	To         string              `json:"to"`
	Total      int64               `json:"total"`
	Partitions []PartitionCountDTO `json:"partitions"`
}

// TopicCountOutput wraps TopicCountBody for Huma.
type TopicCountOutput struct {
	Body TopicCountBody
}

// TopicConsumerGroupsInput identifies the topic to find consumer groups for
// (FUNC-SPEC §8.7 G3).
type TopicConsumerGroupsInput struct {
	Name string `path:"name"`
}

// GroupPartitionLagDTO is one partition of one group's consumption of the
// topic (FUNC-SPEC §8.7 G3).
type GroupPartitionLagDTO struct {
	Partition int32 `json:"partition"`
	Committed int64 `json:"committed"`
	End       int64 `json:"end"`
	Lag       int64 `json:"lag"`
}

// TopicConsumerGroupDTO is one group consuming the topic (FUNC-SPEC §8.7 G3).
type TopicConsumerGroupDTO struct {
	GroupID    string                 `json:"groupId"`
	State      string                 `json:"state"`
	TotalLag   int64                  `json:"totalLag"`
	Partitions []GroupPartitionLagDTO `json:"partitions"`
}

// TopicConsumerGroupsBody is GET /v1/topics/{name}/consumer-groups's response
// body (FUNC-SPEC §8.7 G3).
type TopicConsumerGroupsBody struct {
	Topic  string                  `json:"topic"`
	Groups []TopicConsumerGroupDTO `json:"groups"`
}

// TopicConsumerGroupsOutput wraps TopicConsumerGroupsBody for Huma.
type TopicConsumerGroupsOutput struct {
	Body TopicConsumerGroupsBody
}

// CreateTopicRequestBody is POST /v1/topics's request body (FUNC-SPEC §8.7
// T5), also T6's per-item shape.
type CreateTopicRequestBody struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// CreateTopicInput is POST /v1/topics's parameters (FUNC-SPEC §8.7 T5, §8.2
// `?dryRun=true`).
type CreateTopicInput struct {
	DryRun bool `query:"dryRun"`
	Body   CreateTopicRequestBody
}

// CreateTopicBody is POST /v1/topics's response body: either the created
// topic directly (FUNC-SPEC §8.7 T5), or — when DryRun — the FUNC-SPEC §8.3
// dry-run envelope, Plan carrying the same shape a real create would
// return. Both shapes share this one Go type (with the unused half left at
// its zero value, omitted from JSON) since Huma's typed Output has one Body
// type per operation.
type CreateTopicBody struct {
	DryRun            bool                  `json:"dryRun,omitempty"`
	Plan              *CreateTopicPlanDTO   `json:"plan,omitempty"`
	Name              string                `json:"name,omitempty"`
	Partitions        int32                 `json:"partitions,omitempty"`
	ReplicationFactor int16                 `json:"replicationFactor,omitempty"`
	Configs           []TopicConfigEntryDTO `json:"configs,omitempty"`
}

// CreateTopicPlanDTO is a dry-run T5's plan (the same shape a real create
// would return).
type CreateTopicPlanDTO struct {
	Name              string                `json:"name"`
	Partitions        int32                 `json:"partitions"`
	ReplicationFactor int16                 `json:"replicationFactor"`
	Configs           []TopicConfigEntryDTO `json:"configs"`
}

// CreateTopicOutput wraps CreateTopicBody for Huma. Status is Huma's
// dynamic-status-code field: 201 on a real create, 200 on a dry-run
// (FUNC-SPEC §8.3 dry-run envelope).
type CreateTopicOutput struct {
	Status int
	Body   CreateTopicBody
}

// CreateTopicsBulkRequestBody is POST /v1/batch/topics's request body
// (FUNC-SPEC §8.7 T6).
type CreateTopicsBulkRequestBody struct {
	Topics []CreateTopicRequestBody `json:"topics"`
}

// CreateTopicsBulkInput is POST /v1/batch/topics's parameters.
type CreateTopicsBulkInput struct {
	Body CreateTopicsBulkRequestBody
}

// CreateBulkItemResultDTO is one T6 item's outcome (FUNC-SPEC §8.3 Bulk).
type CreateBulkItemResultDTO struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
	Name   string `json:"name,omitempty"`
	Error  string `json:"error,omitempty"`
}

// TopicBulkSummaryDTO is the bulk envelope's `summary` sub-object
// (FUNC-SPEC §8.3).
type TopicBulkSummaryDTO struct {
	Total  int `json:"total"`
	OK     int `json:"ok"`
	Failed int `json:"failed"`
}

// CreateTopicsBulkBody is POST /v1/batch/topics's response body.
type CreateTopicsBulkBody struct {
	Items   []CreateBulkItemResultDTO `json:"items"`
	Summary TopicBulkSummaryDTO       `json:"summary"`
}

// CreateTopicsBulkOutput wraps CreateTopicsBulkBody for Huma. Status is
// Huma's dynamic-status-code field: 200 when every item succeeded, 207 when
// the outcome is mixed (FUNC-SPEC §8.3 Bulk).
type CreateTopicsBulkOutput struct {
	Status int
	Body   CreateTopicsBulkBody
}

// AlterConfigRequestBody is PATCH /v1/topics/{name}/config's request body
// (FUNC-SPEC §8.7 T9).
type AlterConfigRequestBody struct {
	Confirm string            `json:"confirm"`
	Set     map[string]string `json:"set,omitempty"`
	Reset   []string          `json:"reset,omitempty"`
}

// AlterConfigInput is PATCH /v1/topics/{name}/config's parameters
// (FUNC-SPEC §8.7 T9, §8.2 `?dryRun=true`).
type AlterConfigInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   AlterConfigRequestBody
}

// ConfigChangeDetailDTO is one config key's planned or applied change
// (FUNC-SPEC §8.6 T9). To is "" for a reset.
type ConfigChangeDetailDTO struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
}

// AlterConfigPlanDTO is a dry-run T9's plan (FUNC-SPEC §8.6).
type AlterConfigPlanDTO struct {
	Topic   string                  `json:"topic"`
	Changes []ConfigChangeDetailDTO `json:"changes"`
}

// AlterConfigBody is PATCH /v1/topics/{name}/config's response body: either
// the altered topic's configs directly (FUNC-SPEC §8.7 T9), or — when
// DryRun — the FUNC-SPEC §8.3 dry-run envelope. See CreateTopicBody for why
// both shapes share one Go type.
type AlterConfigBody struct {
	DryRun  bool                  `json:"dryRun,omitempty"`
	Plan    *AlterConfigPlanDTO   `json:"plan,omitempty"`
	Name    string                `json:"name,omitempty"`
	Configs []TopicConfigEntryDTO `json:"configs,omitempty"`
}

// AlterConfigOutput wraps AlterConfigBody for Huma.
type AlterConfigOutput struct {
	Body AlterConfigBody
}

// AddPartitionsRequestBody is POST /v1/topics/{name}/partitions's request
// body (FUNC-SPEC §8.7 T10).
type AddPartitionsRequestBody struct {
	Confirm    string `json:"confirm"`
	Partitions int32  `json:"partitions"`
}

// AddPartitionsInput is POST /v1/topics/{name}/partitions's parameters
// (FUNC-SPEC §8.7 T10, §8.2 `?dryRun=true`).
type AddPartitionsInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   AddPartitionsRequestBody
}

// AddPartitionsPlanDTO is a dry-run T10's plan (FUNC-SPEC §8.6).
type AddPartitionsPlanDTO struct {
	Topic   string `json:"topic"`
	From    int32  `json:"from"`
	To      int32  `json:"to"`
	Warning string `json:"warning"`
}

// AddPartitionsBody is POST /v1/topics/{name}/partitions's response body:
// either the topic's new partition count directly (FUNC-SPEC §8.7 T10), or
// — when DryRun — the FUNC-SPEC §8.3 dry-run envelope. See CreateTopicBody
// for why both shapes share one Go type.
type AddPartitionsBody struct {
	DryRun         bool                  `json:"dryRun,omitempty"`
	Plan           *AddPartitionsPlanDTO `json:"plan,omitempty"`
	Name           string                `json:"name,omitempty"`
	PartitionCount int32                 `json:"partitionCount,omitempty"`
}

// AddPartitionsOutput wraps AddPartitionsBody for Huma.
type AddPartitionsOutput struct {
	Body AddPartitionsBody
}

// DeleteTopicRequestBody is DELETE /v1/topics/{name}'s request body
// (FUNC-SPEC §8.7 T7). Carried in the DELETE body itself (TECH-SPEC R3: some
// intermediaries strip it — a lost body fails closed as 400
// CONFIRMATION_MISMATCH, since Confirm then arrives empty).
type DeleteTopicRequestBody struct {
	Confirm string `json:"confirm"`
}

// DeleteTopicInput is DELETE /v1/topics/{name}'s parameters (FUNC-SPEC §8.7
// T7, §8.2 `?dryRun=true`).
type DeleteTopicInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   DeleteTopicRequestBody
}

// DeleteTopicPlanDTO is a dry-run T7's plan (FUNC-SPEC §8.6).
type DeleteTopicPlanDTO struct {
	Topic          string `json:"topic"`
	Partitions     int32  `json:"partitions"`
	ApproxMessages int64  `json:"approxMessages"`
}

// DeleteTopicBody is DELETE /v1/topics/{name}'s response body: either the
// deleted topic's name directly (FUNC-SPEC §8.7 T7), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope. See CreateTopicBody for why both shapes
// share one Go type.
type DeleteTopicBody struct {
	DryRun  bool                `json:"dryRun,omitempty"`
	Plan    *DeleteTopicPlanDTO `json:"plan,omitempty"`
	Deleted string              `json:"deleted,omitempty"`
}

// DeleteTopicOutput wraps DeleteTopicBody for Huma.
type DeleteTopicOutput struct {
	Body DeleteTopicBody
}

// BulkDeleteRequestBody is POST /v1/batch/topics/delete's request body
// (FUNC-SPEC §8.7 T8): either an explicit Topics list or a Pattern, never
// both (internal/service/topic.BulkDelete resolves Pattern first when set).
// Confirm is the plan token from a prior dry-run (FUNC-SPEC V5) — omitempty
// so a first dry-run call, which is how that token is discovered, need not
// (and cannot) supply it; core.Destructive checks it only on the real
// execute call.
type BulkDeleteRequestBody struct {
	Confirm string   `json:"confirm,omitempty"`
	Topics  []string `json:"topics,omitempty"`
	Pattern string   `json:"pattern,omitempty"`
}

// BulkDeleteInput is POST /v1/batch/topics/delete's parameters (FUNC-SPEC
// §8.7 T8, §8.2 `?dryRun=true`).
type BulkDeleteInput struct {
	DryRun bool `query:"dryRun"`
	Body   BulkDeleteRequestBody
}

// BulkDeletePlanDTO is a dry-run T8's plan (FUNC-SPEC §8.6, V5): Topics is
// the resolved, sorted target list, PlanToken the confirmation the next
// call must echo.
type BulkDeletePlanDTO struct {
	Topics    []string `json:"topics"`
	PlanToken string   `json:"planToken"`
}

// BulkDeleteBody is POST /v1/batch/topics/delete's response body: either the
// bulk envelope (FUNC-SPEC §8.7 T8), or — when DryRun — the FUNC-SPEC §8.3
// dry-run envelope. See CreateTopicBody for why both shapes share one Go type.
type BulkDeleteBody struct {
	DryRun  bool                      `json:"dryRun,omitempty"`
	Plan    *BulkDeletePlanDTO        `json:"plan,omitempty"`
	Items   []CreateBulkItemResultDTO `json:"items,omitempty"`
	Summary *TopicBulkSummaryDTO      `json:"summary,omitempty"`
}

// BulkDeleteOutput wraps BulkDeleteBody for Huma. Status is Huma's
// dynamic-status-code field: 200 on a dry-run or a clean sweep, 207 when the
// outcome is mixed (FUNC-SPEC §8.3 Bulk).
type BulkDeleteOutput struct {
	Status int
	Body   BulkDeleteBody
}

// PartitionDeleteRecordsDetailDTO is one partition's planned truncation
// (FUNC-SPEC §8.6 T11).
type PartitionDeleteRecordsDetailDTO struct {
	Partition             int32 `json:"partition"`
	BeginOffset           int64 `json:"beginOffset"`
	TruncateTo            int64 `json:"truncateTo"`
	ApproxRecordsAffected int64 `json:"approxRecordsAffected"`
}

// DeleteRecordsPlanDTO is a dry-run T11 or T12's plan (FUNC-SPEC §8.6): T12
// reuses this same shape with every partition's TruncateTo at its current
// end offset.
type DeleteRecordsPlanDTO struct {
	Topic      string                            `json:"topic"`
	Partitions []PartitionDeleteRecordsDetailDTO `json:"partitions"`
}

// PartitionWatermarkDTO is one partition's new begin (low watermark) offset
// after a delete-records or purge apply (FUNC-SPEC §8.7 T11, T12).
type PartitionWatermarkDTO struct {
	ID           int32 `json:"id"`
	LowWatermark int64 `json:"lowWatermark"`
}

// DeleteRecordsRequestBody is POST /v1/topics/{name}/delete-records's
// request body (FUNC-SPEC §8.7 T11). Offsets keys are partition numbers as
// decimal strings (JSON object keys are always strings) mapping to the
// truncateTo offset for that partition.
type DeleteRecordsRequestBody struct {
	Confirm string           `json:"confirm"`
	Offsets map[string]int64 `json:"offsets"`
}

// DeleteRecordsInput is POST /v1/topics/{name}/delete-records's parameters
// (FUNC-SPEC §8.7 T11, §8.2 `?dryRun=true`).
type DeleteRecordsInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   DeleteRecordsRequestBody
}

// DeleteRecordsBody is POST /v1/topics/{name}/delete-records's response
// body: either the new per-partition low watermarks directly (FUNC-SPEC
// §8.7 T11), or — when DryRun — the FUNC-SPEC §8.3 dry-run envelope. See
// CreateTopicBody for why both shapes share one Go type.
type DeleteRecordsBody struct {
	DryRun     bool                    `json:"dryRun,omitempty"`
	Plan       *DeleteRecordsPlanDTO   `json:"plan,omitempty"`
	Partitions []PartitionWatermarkDTO `json:"partitions,omitempty"`
}

// DeleteRecordsOutput wraps DeleteRecordsBody for Huma.
type DeleteRecordsOutput struct {
	Body DeleteRecordsBody
}

// PurgeRequestBody is POST /v1/topics/{name}/purge's request body
// (FUNC-SPEC §8.7 T12).
type PurgeRequestBody struct {
	Confirm string `json:"confirm"`
}

// PurgeInput is POST /v1/topics/{name}/purge's parameters (FUNC-SPEC §8.7
// T12, §8.2 `?dryRun=true`).
type PurgeInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   PurgeRequestBody
}

// PurgeBody is POST /v1/topics/{name}/purge's response body: either the new
// per-partition low watermarks directly (FUNC-SPEC §8.7 T12), or — when
// DryRun — the FUNC-SPEC §8.3 dry-run envelope (T11's plan shape). See
// CreateTopicBody for why both shapes share one Go type.
type PurgeBody struct {
	DryRun     bool                    `json:"dryRun,omitempty"`
	Plan       *DeleteRecordsPlanDTO   `json:"plan,omitempty"`
	Partitions []PartitionWatermarkDTO `json:"partitions,omitempty"`
}

// PurgeOutput wraps PurgeBody for Huma.
type PurgeOutput struct {
	Body PurgeBody
}
