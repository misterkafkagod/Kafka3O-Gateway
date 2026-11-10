package gatewayauth

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func TestClassifyRouteMapsEveryKnownRouteAndDeniesTheRest(t *testing.T) {
	for _, tt := range []struct {
		method, path string
		action       authorization.Action
	}{
		{"GET", "/api/v1/cluster", authorization.ActionDescribeCluster},
		{"GET", "/api/v1/topics", authorization.ActionListTopics},
		{"POST", "/api/v1/topics", authorization.ActionCreateTopic},
		{"GET", "/api/v1/consumer-groups", authorization.ActionListConsumerGroups},
		{"GET", "/api/v1/consumer-groups/billing/offset-reset", authorization.ActionPreviewOffsetReset},
		{"GET", "/api/v1/consumer-groups/team%2Fbilling/offset-reset", authorization.ActionPreviewOffsetReset},
		{"POST", "/api/v1/consumer-groups/billing/offset-reset", authorization.ActionResetOffsets},
		{"GET", "/api/v1/connect/connectors", authorization.ActionListConnectors},
		{"POST", "/api/v1/backups", authorization.ActionStartBackup},
		{"GET", "/api/v1/backups", authorization.ActionListBackups},
		{"GET", "/api/v1/metrics", authorization.ActionReadMetrics},
		{"POST", "/api/v1/backups/orders/pause", authorization.ActionPauseBackup},
		{"POST", "/api/v1/backups/orders/resume", authorization.ActionResumeBackup},
		{"DELETE", "/api/v1/backups/orders", authorization.ActionDeleteBackup},
		{"POST", "/api/v1/backups/orders/restore", authorization.ActionRestoreBackup},
		{"POST", "/api/v1/connect/connectors/orders%20sink/pause", authorization.ActionPauseConnector},
		{"POST", "/api/v1/connect/connectors/orders-sink/resume", authorization.ActionResumeConnector},
		{"POST", "/api/v1/connect/connectors/orders-sink/restart", authorization.ActionRestartConnector},
		{"POST", "/api/v1/connect/connectors/orders-sink/tasks/0/restart", authorization.ActionRestartConnectTask},
		{"GET", "/api/v1/topics/orders", authorization.ActionDescribeTopic},
		{"DELETE", "/api/v1/topics/orders", authorization.ActionDeleteTopic},
		{"GET", "/api/v1/topics/orders/messages", authorization.ActionReadMessages},
		{"POST", "/api/v1/topics/orders/messages", authorization.ActionProduceMessage},
		{"GET", "/api/v1/topics/orders/messages/search", authorization.ActionSearchMessages},
		{"GET", "/api/v1/topics/orders/config", authorization.ActionReadTopicConfig},
		{"POST", "/api/v1/topics/orders/config", authorization.ActionAlterTopicConfig},
		{"GET", "/api/v1/brokers/1/config", authorization.ActionReadBrokerConfig},
		{"POST", "/api/v1/brokers/1/config", authorization.ActionAlterBrokerConfig},
		{"POST", "/api/v1/topics/orders/partitions", authorization.ActionIncreasePartitions},
		{"GET", "/api/v1/topics/orders/reassignments", authorization.ActionListReassignments},
		{"POST", "/api/v1/topics/orders/partitions/3/replicas", authorization.ActionReassignReplicas},
	} {
		route, ok := ClassifyRoute(tt.method, tt.path)
		if !ok || route.Anonymous || route.Action != tt.action {
			t.Errorf("%s %s = %+v %t, want %s", tt.method, tt.path, route, ok, tt.action)
		}
	}
	if route, ok := ClassifyRoute("GET", "/api/v1/health/live"); !ok || !route.Anonymous {
		t.Fatalf("liveness = %+v %t", route, ok)
	}
	for _, tt := range [][2]string{
		{"POST", "/api/v1/health/live"}, {"PUT", "/api/v1/topics/orders"}, {"PATCH", "/api/v1/topics"}, {"HEAD", "/api/v1/cluster"},
		{"GET", "/openapi.json"}, {"GET", "/docs"}, {"GET", "/api/v1/"}, {"GET", "/api/v1/topics/"}, {"GET", "/api/v1/topics//messages"},
		{"GET", "/api/v1/topics/../cluster"}, {"GET", "/api/v1/topics/orders/messages/search/extra"}, {"GET", "/api/v2/topics"},
		{"POST", "/api/v1/topics/orders/partitions/3/replicas/x"}, {"GET", "/api/v1/brokers/1"}, {"GET", "/api/v1/admin"},
		{"GET", "/api/v1/consumer-groups/billing"}, {"DELETE", "/api/v1/consumer-groups/billing/offset-reset"},
		{"GET", "/api/v1/consumer-groups//offset-reset"}, {"GET", "/api/v1/consumer-groups/billing/offset-reset/extra"},
		{"GET", "/api/v1/connect"}, {"DELETE", "/api/v1/backups"}, {"PUT", "/api/v1/backups"}, {"GET", "/api/v1/backups/orders"}, {"POST", "/api/v1/backups/orders/restart"}, {"POST", "/api/v1/connect/connectors"}, {"GET", "/api/v1/connect/connectors/sink"}, {"GET", "/api/v1/connect/plugins"},
		{"GET", "/api/v1/connect/connectors/sink/pause"}, {"PUT", "/api/v1/connect/connectors/sink/pause"}, {"DELETE", "/api/v1/connect/connectors/sink"},
		{"POST", "/api/v1/connect/connectors/sink/config"}, {"POST", "/api/v1/connect/connectors/sink/tasks/0"}, {"POST", "/api/v1/connect/connectors/sink/tasks/0/restart/x"},
	} {
		if route, ok := ClassifyRoute(tt[0], tt[1]); ok {
			t.Errorf("%s %s was classified as %+v", tt[0], tt[1], route)
		}
	}
}
