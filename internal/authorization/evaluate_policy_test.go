package authorization

import (
	"errors"
	"testing"
)

func TestEvaluatePolicyKafkaActionMatrix(t *testing.T) {
	reads := []Action{
		ActionDescribeCluster, ActionListTopics, ActionDescribeTopic, ActionReadMessages,
		ActionSearchMessages, ActionListConsumerGroups, ActionPreviewOffsetReset, ActionReadTopicConfig,
		ActionReadBrokerConfig, ActionListReassignments,
	}
	writes := []Action{
		ActionProduceMessage, ActionCreateTopic, ActionDeleteTopic,
		ActionAlterTopicConfig, ActionAlterBrokerConfig, ActionIncreasePartitions,
		ActionReassignReplicas, ActionResetOffsets,
	}
	for _, tt := range []struct {
		name        string
		role        Role
		environment Environment
		reason      string
		read        bool
		write       bool
		denial      Denial
	}{
		{name: "no grant", environment: EnvironmentNonProduction, denial: DenialInsufficientRole},
		{name: "viewer development", role: RoleViewer, environment: EnvironmentNonProduction, read: true, denial: DenialInsufficientRole},
		{name: "viewer production", role: RoleViewer, environment: EnvironmentProduction, read: true, denial: DenialInsufficientRole},
		{name: "operator development", role: RoleOperator, environment: EnvironmentNonProduction, read: true, write: true},
		{name: "operator production", role: RoleOperator, environment: EnvironmentProduction, read: true, denial: DenialProductionAdmin},
		{name: "admin development", role: RoleAdmin, environment: EnvironmentNonProduction, read: true, write: true},
		{name: "admin production with reason", role: RoleAdmin, environment: EnvironmentProduction, reason: "approved change", read: true, write: true},
		{name: "admin production without reason", role: RoleAdmin, environment: EnvironmentProduction, reason: " \t ", read: true, denial: DenialReasonRequired},
		{name: "operator unclassified", role: RoleOperator, environment: EnvironmentUnclassified, read: true, denial: DenialUnclassified},
		{name: "admin unclassified", role: RoleAdmin, environment: EnvironmentUnclassified, reason: "approved", read: true, denial: DenialUnclassified},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var grants []Grant
			if tt.role != RoleNone {
				grants = []Grant{{ClusterID: "a", Role: tt.role}}
			}
			for _, action := range reads {
				got, err := EvaluatePolicy(PolicyRequest{Grants: grants, ClusterID: "a", Environment: tt.environment, Action: action})
				if err != nil || got.Allowed != tt.read || got.EffectiveRole != tt.role {
					t.Errorf("read %q: got %+v, %v", action, got, err)
				}
			}
			for _, action := range writes {
				got, err := EvaluatePolicy(PolicyRequest{Grants: grants, ClusterID: "a", Environment: tt.environment, Action: action, ChangeReason: tt.reason})
				if err != nil || got.Allowed != tt.write || got.EffectiveRole != tt.role || got.Denial != tt.denial {
					t.Errorf("write %q: got %+v, %v; want allowed=%t denial=%q", action, got, err, tt.write, tt.denial)
				}
			}
		})
	}
}

func TestEvaluatePolicyScopedAdministration(t *testing.T) {
	globalActions := []Action{ActionManageAccounts, ActionManageRoleBindings, ActionManageGroupMappings, ActionManageClusters, ActionReadGlobalAudit}
	clusterAdmin := []Grant{{ClusterID: "a", Role: RoleAdmin}}
	for _, action := range globalActions {
		got, err := EvaluatePolicy(PolicyRequest{Grants: clusterAdmin, ClusterID: "a", Action: action})
		if err != nil || got.Allowed || got.Denial != DenialInsufficientRole {
			t.Errorf("cluster admin %q: got %+v, %v", action, got, err)
		}
		got, err = EvaluatePolicy(PolicyRequest{Grants: []Grant{{Role: RoleAdmin}}, Action: action})
		if err != nil || !got.Allowed {
			t.Errorf("global admin %q: got %+v, %v", action, got, err)
		}
	}
	for _, tt := range []struct {
		name   string
		grants []Grant
		target string
		want   bool
	}{
		{name: "assigned admin", grants: clusterAdmin, target: "a", want: true},
		{name: "other cluster", grants: clusterAdmin, target: "b"},
		{name: "global admin", grants: []Grant{{Role: RoleAdmin}}, target: "b", want: true},
		{name: "global operator", grants: []Grant{{Role: RoleOperator}}, target: "a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvaluatePolicy(PolicyRequest{Grants: tt.grants, ClusterID: tt.target, Action: ActionReadClusterAudit})
			if err != nil || got.Allowed != tt.want {
				t.Errorf("cluster audit = %+v, %v; want allowed=%t", got, err, tt.want)
			}
		})
	}
}

func TestEvaluatePolicyFailsClosedOnInvalidInput(t *testing.T) {
	admin := []Grant{{Role: RoleAdmin}}
	for _, tt := range []struct {
		name    string
		request PolicyRequest
		want    error
	}{
		{name: "unknown action", request: PolicyRequest{Grants: admin, ClusterID: "a", Action: "new.write"}, want: ErrUnknownAction},
		{name: "missing cluster", request: PolicyRequest{Grants: admin, ClusterID: " ", Action: ActionReadMessages}, want: ErrMissingCluster},
		{name: "invalid grant", request: PolicyRequest{Grants: []Grant{{Role: "OWNER"}}, ClusterID: "a", Action: ActionReadMessages}, want: ErrInvalidRole},
		{name: "invalid environment", request: PolicyRequest{Grants: admin, ClusterID: "a", Action: ActionCreateTopic, Environment: "prod", ChangeReason: "reason"}, want: ErrInvalidEnvironment},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvaluatePolicy(tt.request)
			if !errors.Is(err, tt.want) || got.Allowed {
				t.Errorf("EvaluatePolicy() = %+v, %v; want %v and no access", got, err, tt.want)
			}
		})
	}
}

func TestEvaluatePolicyCombinesGlobalAndClusterGrants(t *testing.T) {
	grants := []Grant{{Role: RoleViewer}, {ClusterID: "a", Role: RoleAdmin}}
	request := PolicyRequest{Grants: grants, Action: ActionAlterTopicConfig, Environment: EnvironmentProduction, ChangeReason: "approved"}
	for _, clusterID := range []string{"a", "b"} {
		read, err := EvaluatePolicy(PolicyRequest{Grants: grants, ClusterID: clusterID, Action: ActionReadMessages})
		if err != nil || !read.Allowed {
			t.Errorf("cluster %s read: got %+v, %v", clusterID, read, err)
		}
	}
	request.ClusterID = "a"
	got, err := EvaluatePolicy(request)
	if err != nil || !got.Allowed || got.EffectiveRole != RoleAdmin {
		t.Fatalf("cluster a write: got %+v, %v", got, err)
	}
	request.ClusterID = "b"
	got, err = EvaluatePolicy(request)
	if err != nil || got.Allowed || got.EffectiveRole != RoleViewer {
		t.Fatalf("cluster b write: got %+v, %v", got, err)
	}
}

func TestEvaluatePolicyClusterGrantCannotReduceGlobalOperator(t *testing.T) {
	grants := []Grant{{Role: RoleOperator}, {ClusterID: "a", Role: RoleViewer}}
	request := PolicyRequest{Grants: grants, ClusterID: "a", Action: ActionProduceMessage}
	request.Environment = EnvironmentNonProduction
	got, err := EvaluatePolicy(request)
	if err != nil || !got.Allowed || got.EffectiveRole != RoleOperator {
		t.Fatalf("non-production write: got %+v, %v", got, err)
	}
	request.Environment = EnvironmentProduction
	request.ChangeReason = "approved"
	got, err = EvaluatePolicy(request)
	if err != nil || got.Allowed || got.Denial != DenialProductionAdmin {
		t.Fatalf("production write: got %+v, %v", got, err)
	}
}

func TestEvaluatePolicyGlobalAdminCoversEveryCluster(t *testing.T) {
	grants := []Grant{{Role: RoleAdmin}, {ClusterID: "a", Role: RoleViewer}}
	for _, clusterID := range []string{"a", "b"} {
		got, err := EvaluatePolicy(PolicyRequest{
			Grants: grants, ClusterID: clusterID, Action: ActionDeleteTopic,
			Environment: EnvironmentProduction, ChangeReason: "approved",
		})
		if err != nil || !got.Allowed || got.EffectiveRole != RoleAdmin {
			t.Errorf("cluster %s: got %+v, %v", clusterID, got, err)
		}
	}
}
