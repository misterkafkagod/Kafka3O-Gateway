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
