package topic

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// ConfigChangeDetail is one config key's planned change (FUNC-SPEC §8.6 T9):
// To is the empty string for a reset — the fake/real value it reverts to is
// whatever the broker's own default is, not knowable until re-described.
type ConfigChangeDetail struct {
	Name string
	From string
	To   string
}

// AlterConfigPlan is T9's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the topic name, matching §8.6's row for T9.
type AlterConfigPlan struct {
	Topic   string
	Changes []ConfigChangeDetail
}

// ConfirmTarget implements core.Plan.
func (p AlterConfigPlan) ConfirmTarget() string { return p.Topic }

// AlterConfigResult is ApplyAlterConfig's output (FUNC-SPEC §8.7 T9).
type AlterConfigResult struct {
	Name    string
	Configs []kafka.ConfigEntry
}

// PlanAlterConfig resolves topic's current config values for every key in
// set or reset, so the plan's changes[] can show from/to (FUNC-SPEC §8.6 T9).
func (s *Service) PlanAlterConfig(ctx context.Context, topicName string, set map[string]string, reset []string) (AlterConfigPlan, error) {
	current, err := s.admin.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		return AlterConfigPlan{}, err
	}
	byName := make(map[string]kafka.ConfigEntry, len(current))
	for _, c := range current {
		byName[c.Name] = c
	}

	changes := make([]ConfigChangeDetail, 0, len(set)+len(reset))
	for name, value := range set {
		changes = append(changes, ConfigChangeDetail{Name: name, From: byName[name].Value, To: value})
	}
	for _, name := range reset {
		changes = append(changes, ConfigChangeDetail{Name: name, From: byName[name].Value, To: ""})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Name < changes[j].Name })

	return AlterConfigPlan{Topic: topicName, Changes: changes}, nil
}

// ApplyAlterConfig applies set/reset to plan.Topic and re-describes it so
// the response's configs[] carries freshly normalised sources (FUNC-SPEC
// §8.7 T9), matching T2's own DescribeTopicConfigs contract.
func (s *Service) ApplyAlterConfig(ctx context.Context, plan AlterConfigPlan, set map[string]string, reset []string) (AlterConfigResult, error) {
	changes := make([]kafka.ConfigChange, 0, len(set)+len(reset))
	for name, value := range set {
		v := value
		changes = append(changes, kafka.ConfigChange{Name: name, Value: &v})
	}
	for _, name := range reset {
		changes = append(changes, kafka.ConfigChange{Name: name})
	}

	if err := s.admin.IncrementalAlterTopicConfigs(ctx, plan.Topic, changes); err != nil {
		return AlterConfigResult{}, err
	}
	configs, err := s.admin.DescribeTopicConfigs(ctx, plan.Topic)
	if err != nil {
		return AlterConfigResult{}, err
	}
	return AlterConfigResult{Name: plan.Topic, Configs: configs}, nil
}

// AlterConfig sequences T9's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; T9 is in §5.6's destructive set,
// so — unlike T5/T6 — it requires confirm and honours F3).
func (s *Service) AlterConfig(
	ctx context.Context, caller core.Caller, topicName string, set map[string]string, reset []string, confirm string, dryRun bool,
) (core.Result[AlterConfigPlan, AlterConfigResult], error) {
	desc, _ := command.Lookup("T9")
	attempt := s.newEvent(caller, "T9", topicName)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (AlterConfigPlan, error) { return s.PlanAlterConfig(ctx, topicName, set, reset) },
		func(plan AlterConfigPlan) (AlterConfigResult, error) {
			return s.ApplyAlterConfig(ctx, plan, set, reset)
		},
	)
}

// AddPartitionsPlan is T10's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the topic name.
type AddPartitionsPlan struct {
	Topic   string
	From    int32
	To      int32
	Warning string
}

// ConfirmTarget implements core.Plan.
func (p AddPartitionsPlan) ConfirmTarget() string { return p.Topic }

// AddPartitionsResult is ApplyAddPartitions's output (FUNC-SPEC §8.7 T10).
type AddPartitionsResult struct {
	Name           string
	PartitionCount int32
}

// PlanAddPartitions resolves topicName's current partition count and
// checks to is actually an increase (FUNC-SPEC §8.6 T10): to <= from →
// *core.PolicyError{Code: PartitionMismatch}.
func (s *Service) PlanAddPartitions(ctx context.Context, topicName string, to int32) (AddPartitionsPlan, error) {
	t, err := s.admin.DescribeTopics(ctx, topicName)
	if err != nil {
		return AddPartitionsPlan{}, err
	}
	from := int32(len(t.Partitions))
	if to <= from {
		return AddPartitionsPlan{}, &core.PolicyError{
			Code:    core.PartitionMismatch,
			Message: "partitions must be greater than the current count",
		}
	}
	return AddPartitionsPlan{Topic: topicName, From: from, To: to, Warning: "key-to-partition mapping changes"}, nil
}

// ApplyAddPartitions sets plan.Topic's partition count to plan.To
// (FUNC-SPEC §8.7 T10).
func (s *Service) ApplyAddPartitions(ctx context.Context, plan AddPartitionsPlan) (AddPartitionsResult, error) {
	if err := s.admin.CreatePartitions(ctx, plan.Topic, plan.To); err != nil {
		return AddPartitionsResult{}, err
	}
	return AddPartitionsResult{Name: plan.Topic, PartitionCount: plan.To}, nil
}

// AddPartitions sequences T10's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; T10 is in §5.6's destructive set).
func (s *Service) AddPartitions(
	ctx context.Context, caller core.Caller, topicName string, to int32, confirm string, dryRun bool,
) (core.Result[AddPartitionsPlan, AddPartitionsResult], error) {
	desc, _ := command.Lookup("T10")
	attempt := s.newEvent(caller, "T10", topicName)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (AddPartitionsPlan, error) { return s.PlanAddPartitions(ctx, topicName, to) },
		func(plan AddPartitionsPlan) (AddPartitionsResult, error) { return s.ApplyAddPartitions(ctx, plan) },
	)
}
