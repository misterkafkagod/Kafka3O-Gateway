package cluster_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/gates"
)

func testClusterAuditor() (*audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	return audit.NewAuditor(rec), rec
}

func testClusterRunner() core.Runner {
	return core.Runner{Check: gates.Check}
}

func clusterOperatorCaller() core.Caller { return core.Caller{Tier: core.TierOperator} }

func TestClusterService_AlterBrokerConfig_ConfirmIsBrokerIDString(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedBroker(101, "b101", 9092, "")
	f.SeedBrokerConfigs(101, kafka.ConfigEntry{Name: "log.retention.hours", Value: "168", Source: kafka.SourceDefault})
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	plan, err := svc.PlanAlterBrokerConfig(context.Background(), 101, map[string]string{"log.retention.hours": "72"}, nil)
	if err != nil {
		t.Fatalf("PlanAlterBrokerConfig() error: %v", err)
	}
	if plan.ConfirmTarget() != "101" {
		t.Errorf("ConfirmTarget() = %q, want \"101\"", plan.ConfirmTarget())
	}
	if len(plan.Changes) != 1 || plan.Changes[0].From != "168" || plan.Changes[0].To != "72" {
		t.Fatalf("plan.Changes = %+v, want [{log.retention.hours 168 72}]", plan.Changes)
	}

	result, err := svc.AlterBrokerConfig(context.Background(), clusterOperatorCaller(), 101,
		map[string]string{"log.retention.hours": "72"}, nil, "101", false)
	if err != nil {
		t.Fatalf("AlterBrokerConfig() error: %v", err)
	}
	if result.Value.BrokerID != 101 {
		t.Errorf("BrokerID = %d, want 101", result.Value.BrokerID)
	}
}

func TestClusterService_Reassign_PlanMovesAndToken(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-demo", 1)
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	plan, err := svc.PlanReassign(context.Background(), []cluster.ReassignMove{
		{Topic: "t-demo", Partition: 0, Replicas: []int32{1, 2, 3}},
	})
	if err != nil {
		t.Fatalf("PlanReassign() error: %v", err)
	}
	if len(plan.Moves) != 1 || plan.PlanToken == "" {
		t.Fatalf("plan = %+v, want one move and a non-empty token", plan)
	}

	result, err := svc.Reassign(context.Background(), clusterOperatorCaller(),
		[]cluster.ReassignMove{{Topic: "t-demo", Partition: 0, Replicas: []int32{1, 2, 3}}}, plan.PlanToken, false)
	if err != nil {
		t.Fatalf("Reassign() error: %v", err)
	}
	if result.Value.Summary != (core.BulkSummary{Total: 1, Succeeded: 1}) {
		t.Errorf("Summary = %+v, want {Total:1 Succeeded:1}", result.Value.Summary)
	}
}

func TestClusterService_Reassign_StaleTokenMismatch(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-demo", 2)
	auditor, rec := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	stale, err := svc.PlanReassign(context.Background(), []cluster.ReassignMove{
		{Topic: "t-demo", Partition: 0, Replicas: []int32{1, 2}},
	})
	if err != nil {
		t.Fatalf("PlanReassign() error: %v", err)
	}

	_, err = svc.Reassign(context.Background(), clusterOperatorCaller(),
		[]cluster.ReassignMove{
			{Topic: "t-demo", Partition: 0, Replicas: []int32{1, 2}},
			{Topic: "t-demo", Partition: 1, Replicas: []int32{2, 3}},
		}, stale.PlanToken, false)
	if !core.IsCode(err, core.ConfirmationMismatch) {
		t.Fatalf("Reassign(stale token) error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	if len(rec.Events()) != 0 {
		t.Errorf("Events() = %+v, want none", rec.Events())
	}
}

func TestClusterService_Cancel_PlanAndApply(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-demo", 1)
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	move := []cluster.ReassignMove{{Topic: "t-demo", Partition: 0, Replicas: []int32{1, 2, 3}}}
	movePlan, err := svc.PlanReassign(context.Background(), move)
	if err != nil {
		t.Fatalf("PlanReassign() error: %v", err)
	}
	if _, err := svc.Reassign(context.Background(), clusterOperatorCaller(), move, movePlan.PlanToken, false); err != nil {
		t.Fatalf("Reassign() error: %v", err)
	}

	plan, err := svc.PlanCancelReassignments(context.Background(), []kafka.TopicPartition{{Topic: "t-demo", Partition: 0}})
	if err != nil {
		t.Fatalf("PlanCancelReassignments() error: %v", err)
	}
	if len(plan.Moves) != 1 || plan.Moves[0].Replicas != nil {
		t.Fatalf("plan.Moves = %+v, want one move with nil Replicas", plan.Moves)
	}

	result, err := svc.CancelReassignments(context.Background(), clusterOperatorCaller(),
		[]kafka.TopicPartition{{Topic: "t-demo", Partition: 0}}, plan.PlanToken, false)
	if err != nil {
		t.Fatalf("CancelReassignments() error: %v", err)
	}
	if result.Value.Summary.Succeeded != 1 {
		t.Errorf("Summary = %+v, want Succeeded 1", result.Value.Summary)
	}

	reassignments, err := f.ListReassignments(context.Background())
	if err != nil {
		t.Fatalf("ListReassignments() error: %v", err)
	}
	for _, r := range reassignments {
		if r.Topic == "t-demo" && r.Partition == 0 {
			t.Error("ListReassignments() still lists t-demo/0 after cancel")
		}
	}
}

func TestClusterService_Elect_BulkEnvelope(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-demo", 1)
	f.SeedPartitionMeta("t-demo", 0, 1, []int32{1, 2}, []int32{1, 2})
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	plan, err := svc.PlanElect(context.Background(), true, []kafka.TopicPartition{{Topic: "t-demo", Partition: 0}})
	if err != nil {
		t.Fatalf("PlanElect() error: %v", err)
	}

	result, err := svc.Elect(context.Background(), clusterOperatorCaller(), true,
		[]kafka.TopicPartition{{Topic: "t-demo", Partition: 0}}, plan.PlanToken, false)
	if err != nil {
		t.Fatalf("Elect() error: %v", err)
	}
	if result.Value.Summary != (core.BulkSummary{Total: 1, Succeeded: 1}) {
		t.Errorf("Summary = %+v, want {Total:1 Succeeded:1}", result.Value.Summary)
	}
}

func TestClusterService_Import_PlanCreateAlterDeleteUnchanged(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-alter", 1)
	f.SeedTopicConfigs("t-alter", kafka.ConfigEntry{Name: "retention.ms", Value: "3600000", Source: kafka.SourceDefault})
	f.SeedTopic("t-unchanged", 1)
	f.SeedTopicConfigs("t-unchanged", kafka.ConfigEntry{Name: "retention.ms", Value: "60000", Source: kafka.SourceDynamic})
	f.SeedTopic("t-extra", 1)
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	desired := []cluster.ImportTopic{
		{Name: "t-new", Partitions: 1, ReplicationFactor: 1},
		{Name: "t-alter", Partitions: 1, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "60000"}},
		{Name: "t-unchanged", Partitions: 1, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "60000"}},
	}
	plan, err := svc.PlanImport(context.Background(), desired, false)
	if err != nil {
		t.Fatalf("PlanImport() error: %v", err)
	}
	if len(plan.Create) != 1 || plan.Create[0].Name != "t-new" {
		t.Errorf("plan.Create = %+v, want [t-new]", plan.Create)
	}
	if len(plan.Alter) != 1 || plan.Alter[0].Name != "t-alter" {
		t.Errorf("plan.Alter = %+v, want [t-alter]", plan.Alter)
	}
	if len(plan.Unchanged) != 1 || plan.Unchanged[0] != "t-unchanged" {
		t.Errorf("plan.Unchanged = %+v, want [t-unchanged]", plan.Unchanged)
	}
	if len(plan.Delete) != 1 || plan.Delete[0] != "t-extra" {
		t.Errorf("plan.Delete = %+v, want [t-extra]", plan.Delete)
	}
}

func TestClusterService_Import_AllowDeleteFalseSkipsDeletes(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-extra", 1)
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	plan, err := svc.PlanImport(context.Background(), nil, false)
	if err != nil {
		t.Fatalf("PlanImport() error: %v", err)
	}
	result, err := svc.ApplyImport(context.Background(), plan)
	if err != nil {
		t.Fatalf("ApplyImport() error: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Action != "skipped" {
		t.Fatalf("result.Items = %+v, want one skipped item", result.Items)
	}
	if _, err := f.DescribeTopics(context.Background(), "t-extra"); err != nil {
		t.Errorf("t-extra was deleted despite allowDelete=false: DescribeTopics() error: %v", err)
	}
}

func TestClusterService_Import_TokenOverNameAndDefinitionHash(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	a, err := svc.PlanImport(context.Background(), []cluster.ImportTopic{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, false)
	if err != nil {
		t.Fatalf("PlanImport() error: %v", err)
	}
	b, err := svc.PlanImport(context.Background(), []cluster.ImportTopic{{Name: "t", Partitions: 3, ReplicationFactor: 1}}, false)
	if err != nil {
		t.Fatalf("PlanImport() error: %v", err)
	}
	if a.PlanToken == b.PlanToken {
		t.Error("PlanImport() token unchanged despite a different desired definition, want distinct tokens")
	}
}

func TestClusterService_Import_StaleTokenMismatch(t *testing.T) {
	t.Parallel()
	f := fake.New()
	auditor, rec := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	stale, err := svc.PlanImport(context.Background(), []cluster.ImportTopic{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, false)
	if err != nil {
		t.Fatalf("PlanImport() error: %v", err)
	}

	_, err = svc.Import(context.Background(), clusterOperatorCaller(),
		[]cluster.ImportTopic{{Name: "t", Partitions: 3, ReplicationFactor: 1}}, false, stale.PlanToken, false)
	if !core.IsCode(err, core.ConfirmationMismatch) {
		t.Fatalf("Import(stale token) error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	if len(rec.Events()) != 0 {
		t.Errorf("Events() = %+v, want none", rec.Events())
	}
}
