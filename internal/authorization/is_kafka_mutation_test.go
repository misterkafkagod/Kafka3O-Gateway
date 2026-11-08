package authorization

import "testing"

func TestIsKafkaMutation(t *testing.T) {
	for _, action := range []Action{ActionProduceMessage, ActionCreateTopic, ActionDeleteTopic, ActionAlterTopicConfig,
		ActionAlterBrokerConfig, ActionIncreasePartitions, ActionReassignReplicas} {
		if !IsKafkaMutation(action) {
			t.Errorf("%s is not a mutation", action)
		}
	}
	for _, action := range []Action{ActionListTopics, ActionReadMessages, ActionManageAccounts, ActionReadClusterAudit, "unknown"} {
		if IsKafkaMutation(action) {
			t.Errorf("%s is a mutation", action)
		}
	}
}
