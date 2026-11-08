package main

import (
	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/httpapi"
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
	httpapi.RegisterGetTopicConfigurations(api, a.getConfigurations)
	httpapi.RegisterGetBrokerConfigurations(api, a.getConfigurations)
	httpapi.RegisterAlterTopicConfiguration(api, a.alterConfiguration)
	httpapi.RegisterAlterBrokerConfiguration(api, a.alterConfiguration)
	httpapi.RegisterIncreasePartitions(api, a.increasePartitions)
	httpapi.RegisterListReassignments(api, a.listReassignments)
	httpapi.RegisterReassignPartition(api, a.reassignPartition)
}
