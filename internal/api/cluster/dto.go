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
