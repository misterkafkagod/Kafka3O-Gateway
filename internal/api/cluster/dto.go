// Package cluster wires the cluster-administration commands (FUNC-SPEC §8.7
// C1, C2, C4) onto Huma operations: routes, DTOs, and the mapping between
// them and internal/service/cluster's plain domain results (TECH-SPEC I5).
package cluster

// BrokerDTO is one broker in a DescribeCluster response (FUNC-SPEC §8.7 C1).
type BrokerDTO struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack"`
}

// DescribeClusterInput takes no parameters.
type DescribeClusterInput struct{}

// DescribeClusterBody is /v1/cluster's response body (FUNC-SPEC §8.7 C1).
type DescribeClusterBody struct {
	ClusterID    string      `json:"clusterId"`
	ControllerID int32       `json:"controllerId"`
	Brokers      []BrokerDTO `json:"brokers"`
}

// DescribeClusterOutput wraps DescribeClusterBody for Huma.
type DescribeClusterOutput struct {
	Body DescribeClusterBody
}

// BrokerConfigEntryDTO is one configuration property (FUNC-SPEC §8.7 C2, T2). Value
// is nil (JSON null) for a sensitive entry (FUNC-SPEC §8.2).
type BrokerConfigEntryDTO struct {
	Name        string  `json:"name"`
	Value       *string `json:"value"`
	Source      string  `json:"source"`
	IsSensitive bool    `json:"isSensitive"`
	IsReadOnly  bool    `json:"isReadOnly"`
}

// DescribeBrokerConfigInput identifies the broker to describe (FUNC-SPEC
// §8.7 C2).
type DescribeBrokerConfigInput struct {
	BrokerID int32 `path:"brokerId"`
}

// DescribeBrokerConfigBody is the C2 response body.
type DescribeBrokerConfigBody struct {
	BrokerID int32                  `json:"brokerId"`
	Configs  []BrokerConfigEntryDTO `json:"configs"`
}

// DescribeBrokerConfigOutput wraps DescribeBrokerConfigBody for Huma.
type DescribeBrokerConfigOutput struct {
	Body DescribeBrokerConfigBody
}

// ClusterHealthInput takes no parameters.
type ClusterHealthInput struct{}

// BrokersHealthDTO is the C4 response's brokers sub-object.
type BrokersHealthDTO struct {
	Online int `json:"online"`
}

// TopicsHealthDTO is the C4 response's topics sub-object.
type TopicsHealthDTO struct {
	Total int `json:"total"`
}

// PartitionsHealthDTO is the C4 response's partitions sub-object.
type PartitionsHealthDTO struct {
	Total              int `json:"total"`
	UnderReplicated    int `json:"underReplicated"`
	Offline            int `json:"offline"`
	NonPreferredLeader int `json:"nonPreferredLeader"`
}

// AffectedDTO is one partition contributing an issue to the C4 response
// (capped at 1 000 entries — TECH-SPEC C11).
type AffectedDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Issue     string `json:"issue"`
}

// ClusterHealthBody is /v1/cluster/health's response body (FUNC-SPEC §8.7
// C4; TECH-SPEC C11).
type ClusterHealthBody struct {
	Brokers    BrokersHealthDTO    `json:"brokers"`
	Topics     TopicsHealthDTO     `json:"topics"`
	Partitions PartitionsHealthDTO `json:"partitions"`
	Affected   []AffectedDTO       `json:"affected"`
	Truncated  bool                `json:"truncated"`
}

// ClusterHealthOutput wraps ClusterHealthBody for Huma.
type ClusterHealthOutput struct {
	Body ClusterHealthBody
}

// QuorumReplicaStateDTO is one voter or observer's replication state
// (FUNC-SPEC §8.7 C6).
type QuorumReplicaStateDTO struct {
	ID           int32 `json:"id"`
	LogEndOffset int64 `json:"logEndOffset"`
	LagMs        int64 `json:"lagMs"`
}

// QuorumInput takes no parameters.
type QuorumInput struct{}

// QuorumBody is GET /v1/cluster/quorum's response body (FUNC-SPEC §8.7 C6).
type QuorumBody struct {
	LeaderID  int32                   `json:"leaderId"`
	Epoch     int32                   `json:"epoch"`
	Voters    []QuorumReplicaStateDTO `json:"voters"`
	Observers []QuorumReplicaStateDTO `json:"observers"`
}

// QuorumOutput wraps QuorumBody for Huma.
type QuorumOutput struct {
	Body QuorumBody
}

// ReassignmentDTO is one partition with a reassignment in progress
// (FUNC-SPEC §8.7 C7).
type ReassignmentDTO struct {
	Topic            string  `json:"topic"`
	Partition        int32   `json:"partition"`
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"addingReplicas"`
	RemovingReplicas []int32 `json:"removingReplicas"`
}

// ListReassignmentsInput takes no parameters.
type ListReassignmentsInput struct{}

// ListReassignmentsBody is GET /v1/cluster/reassignments's response body
// (FUNC-SPEC §8.7 C7).
type ListReassignmentsBody struct {
	Items []ReassignmentDTO `json:"items"`
}

// ListReassignmentsOutput wraps ListReassignmentsBody for Huma.
type ListReassignmentsOutput struct {
	Body ListReassignmentsBody
}

// BrokerLogDirDTO is one broker's one log directory's aggregate usage
// (FUNC-SPEC §8.7 C8).
type BrokerLogDirDTO struct {
	BrokerID   int32  `json:"brokerId"`
	LogDir     string `json:"logDir"`
	TotalBytes int64  `json:"totalBytes"`
	Partitions int    `json:"partitions"`
}

// LogDirsInput is GET /v1/cluster/log-dirs's parameters (FUNC-SPEC §8.7 C8
// "brokerId?"). BrokerID is -1 (its default) when omitted — every broker —
// since Huma v2 does not support pointers for query parameters.
type LogDirsInput struct {
	BrokerID int32 `query:"brokerId" default:"-1"`
}

// LogDirsBody is GET /v1/cluster/log-dirs's response body.
type LogDirsBody struct {
	Items []BrokerLogDirDTO `json:"items"`
}

// LogDirsOutput wraps LogDirsBody for Huma.
type LogDirsOutput struct {
	Body LogDirsBody
}

// ThroughputInput is GET /v1/cluster/throughput's parameters (FUNC-SPEC
// §8.7 C10; §8.8 seconds default 5, ceiling 60 — enforced in the handler,
// not the schema, since the ceiling is configured).
type ThroughputInput struct {
	Topic   string `query:"topic"`
	Seconds int    `query:"seconds"`
}

// ThroughputItemDTO is one topic's message rate over the sample
// (FUNC-SPEC §8.7 C10).
type ThroughputItemDTO struct {
	Topic             string           `json:"topic"`
	MessagesPerSecond float64          `json:"messagesPerSecond"`
	StartEndOffsets   map[string]int64 `json:"startEndOffsets"`
	FinishEndOffsets  map[string]int64 `json:"finishEndOffsets"`
}

// ThroughputBody is GET /v1/cluster/throughput's response body.
type ThroughputBody struct {
	Seconds int                 `json:"seconds"`
	Items   []ThroughputItemDTO `json:"items"`
}

// ThroughputOutput wraps ThroughputBody for Huma.
type ThroughputOutput struct {
	Body ThroughputBody
}

// ExportTopicDTO is one topic's exported definition (FUNC-SPEC §8.7 C11) —
// the same shape C12's `topics[]` request field takes.
type ExportTopicDTO struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// ExportInput is GET /v1/cluster/export's parameters (FUNC-SPEC §8.7 C11).
type ExportInput struct {
	Pattern string `query:"pattern"`
}

// ExportBody is GET /v1/cluster/export's response body.
type ExportBody struct {
	ExportedAt string           `json:"exportedAt"`
	Topics     []ExportTopicDTO `json:"topics"`
}

// ExportOutput wraps ExportBody for Huma.
type ExportOutput struct {
	Body ExportBody
}

// AlterBrokerConfigRequestBody is PATCH /v1/cluster/brokers/{brokerId}/config's
// request body (FUNC-SPEC §8.7 C5).
type AlterBrokerConfigRequestBody struct {
	Confirm string            `json:"confirm"`
	Set     map[string]string `json:"set,omitempty"`
	Reset   []string          `json:"reset,omitempty"`
}

// AlterBrokerConfigInput is PATCH /v1/cluster/brokers/{brokerId}/config's
// parameters (FUNC-SPEC §8.7 C5, §8.2 `?dryRun=true`).
type AlterBrokerConfigInput struct {
	BrokerID int32 `path:"brokerId"`
	DryRun   bool  `query:"dryRun"`
	Body     AlterBrokerConfigRequestBody
}

// ClusterConfigChangeDetailDTO is one config key's planned or applied change
// (FUNC-SPEC §8.6 C5, C12). To is "" for a reset.
type ClusterConfigChangeDetailDTO struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
}

// AlterBrokerConfigPlanDTO is a dry-run C5's plan (FUNC-SPEC §8.6).
type AlterBrokerConfigPlanDTO struct {
	BrokerID string                         `json:"brokerId"`
	Changes  []ClusterConfigChangeDetailDTO `json:"changes"`
}

// AlterBrokerConfigBody is PATCH .../config's response body: either the
// altered broker's configs directly (FUNC-SPEC §8.7 C5), or — when DryRun —
// the FUNC-SPEC §8.3 dry-run envelope. See topic.CreateTopicBody (a sibling
// package) for why both shapes share one Go type.
type AlterBrokerConfigBody struct {
	DryRun   bool                      `json:"dryRun,omitempty"`
	Plan     *AlterBrokerConfigPlanDTO `json:"plan,omitempty"`
	BrokerID int32                     `json:"brokerId,omitempty"`
	Configs  []BrokerConfigEntryDTO    `json:"configs,omitempty"`
}

// AlterBrokerConfigOutput wraps AlterBrokerConfigBody for Huma.
type AlterBrokerConfigOutput struct {
	Body AlterBrokerConfigBody
}

// BulkItemResultDTO is one item's outcome from a C9 bulk operation
// (FUNC-SPEC §8.3 Bulk) — shared by reassign, cancel, and elect.
type BulkItemResultDTO struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ClusterBulkSummaryDTO is the bulk envelope's `summary` sub-object (FUNC-SPEC §8.3).
type ClusterBulkSummaryDTO struct {
	Total  int `json:"total"`
	OK     int `json:"ok"`
	Failed int `json:"failed"`
}

// ReassignMoveDTO is one partition's desired replica set (FUNC-SPEC §8.7 C9
// reassign).
type ReassignMoveDTO struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
}

// ReassignRequestBody is POST /v1/cluster/reassignments's request body
// (FUNC-SPEC §8.7 C9 reassign). Confirm is the plan token from a prior
// dry-run (FUNC-SPEC V5) — omitempty so a first dry-run call, which is how
// that token is discovered, need not (and cannot) supply it.
type ReassignRequestBody struct {
	Confirm       string            `json:"confirm,omitempty"`
	Reassignments []ReassignMoveDTO `json:"reassignments"`
}

// ReassignInput is POST /v1/cluster/reassignments's parameters (FUNC-SPEC
// §8.7 C9, §8.2 `?dryRun=true`).
type ReassignInput struct {
	DryRun bool `query:"dryRun"`
	Body   ReassignRequestBody
}

// ReassignPlanDTO is a dry-run C9 reassign's or cancel's plan (FUNC-SPEC
// §8.6, V5).
type ReassignPlanDTO struct {
	Moves     []ReassignMoveDTO `json:"moves"`
	PlanToken string            `json:"planToken"`
}

// ReassignBody is POST /v1/cluster/reassignments's (and .../cancel's)
// response body: either the bulk envelope (FUNC-SPEC §8.7 C9), or — when
// DryRun — the FUNC-SPEC §8.3 dry-run envelope.
type ReassignBody struct {
	DryRun  bool                   `json:"dryRun,omitempty"`
	Plan    *ReassignPlanDTO       `json:"plan,omitempty"`
	Items   []BulkItemResultDTO    `json:"items,omitempty"`
	Summary *ClusterBulkSummaryDTO `json:"summary,omitempty"`
}

// ReassignOutput wraps ReassignBody for Huma. Status is Huma's
// dynamic-status-code field: 200 on a dry-run or a clean sweep, 207 when
// the outcome is mixed (FUNC-SPEC §8.3 Bulk).
type ReassignOutput struct {
	Status int
	Body   ReassignBody
}

// CancelReassignmentTargetDTO is one partition to cancel a reassignment for
// (FUNC-SPEC §8.7 C9 cancel).
type CancelReassignmentTargetDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// CancelReassignmentsRequestBody is POST /v1/cluster/reassignments/cancel's
// request body (FUNC-SPEC §8.7 C9 cancel).
type CancelReassignmentsRequestBody struct {
	Confirm string                        `json:"confirm,omitempty"`
	Cancel  []CancelReassignmentTargetDTO `json:"cancel"`
}

// CancelReassignmentsInput is POST /v1/cluster/reassignments/cancel's
// parameters (FUNC-SPEC §8.7 C9, §8.2 `?dryRun=true`).
type CancelReassignmentsInput struct {
	DryRun bool `query:"dryRun"`
	Body   CancelReassignmentsRequestBody
}

// ElectTargetDTO is one partition to elect a leader for (FUNC-SPEC §8.7 C9 elect).
type ElectTargetDTO struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// ElectSpecDTO is C9 elect's nested `elect` request field (FUNC-SPEC §8.7 C9).
type ElectSpecDTO struct {
	Type       string           `json:"type"`
	Partitions []ElectTargetDTO `json:"partitions"`
}

// ElectionsRequestBody is POST /v1/cluster/elections's request body
// (FUNC-SPEC §8.7 C9 elect).
type ElectionsRequestBody struct {
	Confirm string       `json:"confirm,omitempty"`
	Elect   ElectSpecDTO `json:"elect"`
}

// ElectionsInput is POST /v1/cluster/elections's parameters (FUNC-SPEC §8.7
// C9, §8.2 `?dryRun=true`).
type ElectionsInput struct {
	DryRun bool `query:"dryRun"`
	Body   ElectionsRequestBody
}

// ElectPlanDTO is a dry-run C9 elect's plan (FUNC-SPEC §8.6, V5).
type ElectPlanDTO struct {
	Elections []ElectTargetDTO `json:"elections"`
	PlanToken string           `json:"planToken"`
}

// ElectionsBody is POST /v1/cluster/elections's response body: either the
// bulk envelope (FUNC-SPEC §8.7 C9), or — when DryRun — the FUNC-SPEC §8.3
// dry-run envelope.
type ElectionsBody struct {
	DryRun  bool                   `json:"dryRun,omitempty"`
	Plan    *ElectPlanDTO          `json:"plan,omitempty"`
	Items   []BulkItemResultDTO    `json:"items,omitempty"`
	Summary *ClusterBulkSummaryDTO `json:"summary,omitempty"`
}

// ElectionsOutput wraps ElectionsBody for Huma. Status is Huma's
// dynamic-status-code field: 200 on a dry-run or a clean sweep, 207 when
// the outcome is mixed (FUNC-SPEC §8.3 Bulk).
type ElectionsOutput struct {
	Status int
	Body   ElectionsBody
}

// ApplyTopicsRequestBody is POST /v1/batch/topics/apply's request body
// (FUNC-SPEC §8.7 C12). Confirm is the plan token from a prior dry-run
// (FUNC-SPEC V5) — omitempty for the same reason ReassignRequestBody's is.
type ApplyTopicsRequestBody struct {
	Confirm     string           `json:"confirm,omitempty"`
	AllowDelete bool             `json:"allowDelete,omitempty"`
	Topics      []ExportTopicDTO `json:"topics"`
}

// ApplyTopicsInput is POST /v1/batch/topics/apply's parameters (FUNC-SPEC
// §8.7 C12, §8.2 `?dryRun=true`).
type ApplyTopicsInput struct {
	DryRun bool `query:"dryRun"`
	Body   ApplyTopicsRequestBody
}

// ImportAlterActionDTO is one existing topic C12 will alter (FUNC-SPEC §8.6
// C12 "alter").
type ImportAlterActionDTO struct {
	Name    string                         `json:"name"`
	Changes []ClusterConfigChangeDetailDTO `json:"changes"`
}

// ApplyTopicsPlanDTO is a dry-run C12's plan (FUNC-SPEC §8.6, V5).
type ApplyTopicsPlanDTO struct {
	Create    []ExportTopicDTO       `json:"create"`
	Alter     []ImportAlterActionDTO `json:"alter"`
	Delete    []string               `json:"delete"`
	Unchanged []string               `json:"unchanged"`
	PlanToken string                 `json:"planToken"`
}

// ApplyTopicsItemResultDTO is one topic's outcome of an executed C12 import
// (FUNC-SPEC §8.7 C12): Status is "ok", "failed", or "skipped" (a delete
// candidate when the request's AllowDelete is false).
type ApplyTopicsItemResultDTO struct {
	Name   string `json:"name"`
	Action string `json:"action"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ApplyTopicsBody is POST /v1/batch/topics/apply's response body: either
// the executed reconciliation's outcome (FUNC-SPEC §8.7 C12), or — when
// DryRun — the FUNC-SPEC §8.3 dry-run envelope.
type ApplyTopicsBody struct {
	DryRun  bool                       `json:"dryRun,omitempty"`
	Plan    *ApplyTopicsPlanDTO        `json:"plan,omitempty"`
	Items   []ApplyTopicsItemResultDTO `json:"items,omitempty"`
	Summary *ClusterBulkSummaryDTO     `json:"summary,omitempty"`
}

// ApplyTopicsOutput wraps ApplyTopicsBody for Huma. Status is Huma's
// dynamic-status-code field: 200 on a dry-run or a clean sweep, 207 when
// the outcome is mixed (FUNC-SPEC §8.3 Bulk).
type ApplyTopicsOutput struct {
	Status int
	Body   ApplyTopicsBody
}
