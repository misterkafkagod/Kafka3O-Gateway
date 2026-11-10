package gatewayauth

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"

// routeActions maps "METHOD route-template" to its P49 action; templates use {} for path parameters.
var routeActions = map[string]authorization.Action{
	"GET cluster":                           authorization.ActionDescribeCluster,
	"GET topics":                            authorization.ActionListTopics,
	"POST topics":                           authorization.ActionCreateTopic,
	"GET consumer-groups":                   authorization.ActionListConsumerGroups,
	"GET consumer-groups/{}/offset-reset":   authorization.ActionPreviewOffsetReset,
	"GET topics/{}":                         authorization.ActionDescribeTopic,
	"DELETE topics/{}":                      authorization.ActionDeleteTopic,
	"GET topics/{}/messages":                authorization.ActionReadMessages,
	"POST topics/{}/messages":               authorization.ActionProduceMessage,
	"GET topics/{}/messages/search":         authorization.ActionSearchMessages,
	"GET topics/{}/config":                  authorization.ActionReadTopicConfig,
	"POST topics/{}/config":                 authorization.ActionAlterTopicConfig,
	"GET brokers/{}/config":                 authorization.ActionReadBrokerConfig,
	"POST brokers/{}/config":                authorization.ActionAlterBrokerConfig,
	"POST topics/{}/partitions":             authorization.ActionIncreasePartitions,
	"GET topics/{}/reassignments":           authorization.ActionListReassignments,
	"POST topics/{}/partitions/{}/replicas": authorization.ActionReassignReplicas,
}
