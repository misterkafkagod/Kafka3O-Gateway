package main

import (
	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

// gatewayActions holds every Kafka action the Gateway API exposes.
type gatewayActions struct {
	getCluster              cluster.GetCluster
	listTopics              topics.ListTopics
	getTopicDetail          topics.GetTopicDetail
	listLatestTopicMessages topics.ListLatestTopicMessages
	readTopicMessages       topics.ReadTopicMessages
	searchTopicMessages     topics.SearchTopicMessages
	produceTopicMessage     topics.ProduceTopicMessage
	createTopic             topics.CreateTopic
	deleteTopic             topics.DeleteTopic
	listConsumerGroups      groups.ListConsumerGroups
	previewOffsetReset      groups.PreviewOffsetReset
	applyOffsetReset        groups.ApplyOffsetReset
	listConnectors          connect.ListConnectors
	manageConnector         connect.ManageConnector
	startBackup             backup.StartBackup
	listBackups             backup.ListBackups
	manageBackup            backup.ManageBackup
	startRestore            backup.StartRestore
	getMetrics              metrics.GetMetrics
	getConfigurations       settings.GetConfigurations
	alterConfiguration      settings.AlterConfiguration
	increasePartitions      partitions.IncreasePartitionCount
	listReassignments       partitions.ListReassignments
	reassignPartition       partitions.ReassignPartition
}

// registerGatewayRoutes is composition-root wiring of every Gateway route; secured mode must classify each one.
func registerGatewayRoutes(api huma.API, a gatewayActions) {
	httpapi.RegisterLive(api)
	httpapi.RegisterGetCluster(api, a.getCluster)
	httpapi.RegisterListTopics(api, a.listTopics)
	httpapi.RegisterGetTopicDetail(api, a.getTopicDetail)
	httpapi.RegisterListLatestTopicMessages(api, a.listLatestTopicMessages, a.readTopicMessages)
	httpapi.RegisterSearchTopicMessages(api, a.searchTopicMessages)
	httpapi.RegisterProduceTopicMessage(api, a.produceTopicMessage)
	httpapi.RegisterCreateTopic(api, a.createTopic)
	httpapi.RegisterDeleteTopic(api, a.deleteTopic)
	httpapi.RegisterListConsumerGroups(api, a.listConsumerGroups)
	httpapi.RegisterPreviewOffsetReset(api, a.previewOffsetReset)
	httpapi.RegisterApplyOffsetReset(api, a.applyOffsetReset)
	httpapi.RegisterListConnectors(api, a.listConnectors)
	httpapi.RegisterConnectorActions(api, a.manageConnector)
	httpapi.RegisterStartBackup(api, a.startBackup)
	httpapi.RegisterListBackups(api, a.listBackups)
	httpapi.RegisterBackupActions(api, a.manageBackup)
	httpapi.RegisterStartRestore(api, a.startRestore)
	httpapi.RegisterGetMetrics(api, a.getMetrics)
	httpapi.RegisterGetTopicConfigurations(api, a.getConfigurations)
	httpapi.RegisterGetBrokerConfigurations(api, a.getConfigurations)
	httpapi.RegisterAlterTopicConfiguration(api, a.alterConfiguration)
	httpapi.RegisterAlterBrokerConfiguration(api, a.alterConfiguration)
	httpapi.RegisterIncreasePartitions(api, a.increasePartitions)
	httpapi.RegisterListReassignments(api, a.listReassignments)
	httpapi.RegisterReassignPartition(api, a.reassignPartition)
}
