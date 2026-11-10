package authorization

import "errors"

type Role string

const (
	RoleNone     Role = ""
	RoleViewer   Role = "VIEWER"
	RoleOperator Role = "OPERATOR"
	RoleAdmin    Role = "ADMIN"
)

type Environment string

const (
	EnvironmentUnclassified  Environment = ""
	EnvironmentNonProduction Environment = "NON_PRODUCTION"
	EnvironmentProduction    Environment = "PRODUCTION"
)

type Action string

const (
	ActionDescribeCluster     Action = "cluster.describe"
	ActionListTopics          Action = "topics.list"
	ActionDescribeTopic       Action = "topics.describe"
	ActionReadMessages        Action = "messages.read"
	ActionSearchMessages      Action = "messages.search"
	ActionListConsumerGroups  Action = "consumer_groups.list"
	ActionPreviewOffsetReset  Action = "consumer_groups.offsets.preview"
	ActionListConnectors      Action = "connect.connectors.list"
	ActionListBackups         Action = "backups.list"
	ActionReadTopicConfig     Action = "topics.config.read"
	ActionReadBrokerConfig    Action = "brokers.config.read"
	ActionListReassignments   Action = "topics.reassignments.list"
	ActionProduceMessage      Action = "messages.produce"
	ActionCreateTopic         Action = "topics.create"
	ActionDeleteTopic         Action = "topics.delete"
	ActionAlterTopicConfig    Action = "topics.config.alter"
	ActionAlterBrokerConfig   Action = "brokers.config.alter"
	ActionIncreasePartitions  Action = "topics.partitions.increase"
	ActionReassignReplicas    Action = "topics.replicas.reassign"
	ActionResetOffsets        Action = "consumer_groups.offsets.reset"
	ActionPauseConnector      Action = "connect.connectors.pause"
	ActionResumeConnector     Action = "connect.connectors.resume"
	ActionRestartConnector    Action = "connect.connectors.restart"
	ActionRestartConnectTask  Action = "connect.tasks.restart"
	ActionStartBackup         Action = "backups.start"
	ActionPauseBackup         Action = "backups.pause"
	ActionResumeBackup        Action = "backups.resume"
	ActionDeleteBackup        Action = "backups.delete"
	ActionManageAccounts      Action = "console.accounts.manage"
	ActionManageRoleBindings  Action = "console.role_bindings.manage"
	ActionManageGroupMappings Action = "console.group_mappings.manage"
	ActionManageClusters      Action = "console.clusters.manage"
	ActionReadGlobalAudit     Action = "console.audit.global.read"
	ActionReadClusterAudit    Action = "console.audit.cluster.read"
)

type Grant struct {
	// An empty ClusterID is a global grant.
	ClusterID string
	Role      Role
}

type PolicyRequest struct {
	Grants       []Grant
	ClusterID    string
	Environment  Environment
	Action       Action
	ChangeReason string
}

type Denial string

const (
	DenialInsufficientRole Denial = "insufficient_role"
	DenialUnclassified     Denial = "environment_unclassified"
	DenialProductionAdmin  Denial = "production_admin_required"
	DenialReasonRequired   Denial = "change_reason_required"
)

type Decision struct {
	Allowed       bool
	EffectiveRole Role
	Denial        Denial
}

var (
	ErrInvalidRole        = errors.New("invalid role grant")
	ErrInvalidEnvironment = errors.New("invalid cluster environment")
	ErrMissingCluster     = errors.New("cluster is required for this action")
	ErrUnknownAction      = errors.New("unknown authorization action")
)

type actionCategory uint8

const (
	clusterRead actionCategory = iota + 1
	kafkaWrite
	clusterAuditRead
	globalAdministration
)
