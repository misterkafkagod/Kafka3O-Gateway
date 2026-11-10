package authorization

func classifyAction(action Action) (actionCategory, error) {
	switch action {
	case ActionDescribeCluster, ActionListTopics, ActionDescribeTopic,
		ActionReadMessages, ActionSearchMessages, ActionListConsumerGroups,
		ActionReadTopicConfig, ActionReadBrokerConfig, ActionListReassignments, ActionPreviewOffsetReset,
		ActionListConnectors:
		return clusterRead, nil
	case ActionProduceMessage, ActionCreateTopic, ActionDeleteTopic,
		ActionAlterTopicConfig, ActionAlterBrokerConfig, ActionIncreasePartitions,
		ActionReassignReplicas, ActionResetOffsets:
		return kafkaWrite, nil
	case ActionReadClusterAudit:
		return clusterAuditRead, nil
	case ActionManageAccounts, ActionManageRoleBindings, ActionManageGroupMappings,
		ActionManageClusters, ActionReadGlobalAudit:
		return globalAdministration, nil
	default:
		return 0, ErrUnknownAction
	}
}
