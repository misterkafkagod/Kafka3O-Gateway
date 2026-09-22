package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// ConfigChangeDetail is one config key's planned or applied change
// (FUNC-SPEC §8.6 C5, C12). To is "" for a reset.
type ConfigChangeDetail struct {
	Name string
	From string
	To   string
}

// AlterBrokerConfigPlan is C5's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget
// is the broker id, as a string (FUNC-SPEC §8.6 C5 row).
type AlterBrokerConfigPlan struct {
	BrokerID string
	Changes  []ConfigChangeDetail
}

// ConfirmTarget implements core.Plan.
func (p AlterBrokerConfigPlan) ConfirmTarget() string { return p.BrokerID }

// AlterBrokerConfigResult is ApplyAlterBrokerConfig's output (FUNC-SPEC §8.7 C5).
type AlterBrokerConfigResult struct {
	BrokerID int32
	Configs  []kafka.ConfigEntry
}

// PlanAlterBrokerConfig resolves brokerID's current config values for every
// key in set or reset, so the plan's changes[] can show from/to (FUNC-SPEC
// §8.6 C5).
func (s *Service) PlanAlterBrokerConfig(ctx context.Context, brokerID int32, set map[string]string, reset []string) (AlterBrokerConfigPlan, error) {
	current, err := s.admin.DescribeBrokerConfigs(ctx, brokerID)
	if err != nil {
		return AlterBrokerConfigPlan{}, err
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

	return AlterBrokerConfigPlan{BrokerID: strconv.Itoa(int(brokerID)), Changes: changes}, nil
}

// ApplyAlterBrokerConfig applies set/reset to brokerID and re-describes it
// so the response's configs[] carries freshly normalised sources
// (FUNC-SPEC §8.7 C5).
func (s *Service) ApplyAlterBrokerConfig(ctx context.Context, brokerID int32, set map[string]string, reset []string) (AlterBrokerConfigResult, error) {
	changes := make([]kafka.ConfigChange, 0, len(set)+len(reset))
	for name, value := range set {
		v := value
		changes = append(changes, kafka.ConfigChange{Name: name, Value: &v})
	}
	for _, name := range reset {
		changes = append(changes, kafka.ConfigChange{Name: name})
	}

	if err := s.admin.IncrementalAlterBrokerConfigs(ctx, brokerID, changes); err != nil {
		return AlterBrokerConfigResult{}, err
	}
	configs, err := s.admin.DescribeBrokerConfigs(ctx, brokerID)
	if err != nil {
		return AlterBrokerConfigResult{}, err
	}
	return AlterBrokerConfigResult{BrokerID: brokerID, Configs: configs}, nil
}

// AlterBrokerConfig sequences C5's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; C5 is in §5.6's destructive set).
func (s *Service) AlterBrokerConfig(
	ctx context.Context, caller core.Caller, brokerID int32, set map[string]string, reset []string, confirm string, dryRun bool,
) (core.Result[AlterBrokerConfigPlan, AlterBrokerConfigResult], error) {
	desc, _ := command.Lookup("C5")
	attempt := s.newEvent(caller, "C5", strconv.Itoa(int(brokerID)))
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (AlterBrokerConfigPlan, error) { return s.PlanAlterBrokerConfig(ctx, brokerID, set, reset) },
		func(AlterBrokerConfigPlan) (AlterBrokerConfigResult, error) {
			return s.ApplyAlterBrokerConfig(ctx, brokerID, set, reset)
		},
	)
}

// reassignTarget renders one topic-partition as a plan-token target string
// (TECH-SPEC C8's general rule — C9 gets no special per-target format the
// way C12 does).
func reassignTarget(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

// ReassignMove is one partition's desired replica set (FUNC-SPEC §8.7 C9
// reassign) — or, with a nil Replicas, a cancel of that partition's
// in-progress reassignment (C9 cancel).
type ReassignMove struct {
	Topic     string
	Partition int32
	Replicas  []int32
}

// ReassignPlan is C9 reassign's and cancel's dry-run plan (FUNC-SPEC §8.6,
// V5). ConfirmTarget is the plan token over the sorted, de-duplicated
// topic-partition targets.
type ReassignPlan struct {
	Moves     []ReassignMove
	PlanToken string
}

// ConfirmTarget implements core.Plan.
func (p ReassignPlan) ConfirmTarget() string { return p.PlanToken }

// PlanReassign resolves moves' plan token, sorted by topic then partition
// for a stable response (FUNC-SPEC §8.6 C9).
func (s *Service) PlanReassign(ctx context.Context, moves []ReassignMove) (ReassignPlan, error) {
	sorted := append([]ReassignMove(nil), moves...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Topic != sorted[j].Topic {
			return sorted[i].Topic < sorted[j].Topic
		}
		return sorted[i].Partition < sorted[j].Partition
	})
	targets := make([]string, len(sorted))
	for i, m := range sorted {
		targets[i] = reassignTarget(m.Topic, m.Partition)
	}
	return ReassignPlan{Moves: sorted, PlanToken: core.Token("C9", targets)}, nil
}

// PlanCancelReassignments resolves the same ReassignPlan shape as
// PlanReassign, with every move's Replicas nil (FUNC-SPEC §8.6 C9 cancel).
func (s *Service) PlanCancelReassignments(ctx context.Context, targets []kafka.TopicPartition) (ReassignPlan, error) {
	sorted := append([]kafka.TopicPartition(nil), targets...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Topic != sorted[j].Topic {
			return sorted[i].Topic < sorted[j].Topic
		}
		return sorted[i].Partition < sorted[j].Partition
	})
	moves := make([]ReassignMove, len(sorted))
	tokenTargets := make([]string, len(sorted))
	for i, tp := range sorted {
		moves[i] = ReassignMove{Topic: tp.Topic, Partition: tp.Partition}
		tokenTargets[i] = reassignTarget(tp.Topic, tp.Partition)
	}
	return ReassignPlan{Moves: moves, PlanToken: core.Token("C9", tokenTargets)}, nil
}

// ApplyReassign moves (or cancels, for a nil Replicas) every planned
// partition's assignment in one port call, converting each one's own
// per-partition result into a bulk item (FUNC-SPEC §8.7 C9).
func (s *Service) ApplyReassign(ctx context.Context, plan ReassignPlan) (core.BulkResult, error) {
	moves := make(map[kafka.TopicPartition][]int32, len(plan.Moves))
	for _, m := range plan.Moves {
		moves[kafka.TopicPartition{Topic: m.Topic, Partition: m.Partition}] = m.Replicas
	}
	results, err := s.admin.AlterPartitionAssignments(ctx, moves)
	if err != nil {
		return core.BulkResult{}, err
	}
	return toReassignBulkResult(results), nil
}

// toReassignBulkResult converts the port's per-partition results into the
// wire bulk envelope shape (FUNC-SPEC §8.3 Bulk).
func toReassignBulkResult(results []kafka.ReassignResult) core.BulkResult {
	items := make([]core.BulkItemResult, len(results))
	summary := core.BulkSummary{Total: len(results)}
	for i, r := range results {
		if r.Err != nil {
			items[i] = core.BulkItemResult{Index: i, Outcome: audit.OutcomeFailed, Error: r.Err.Error()}
			summary.Failed++
			continue
		}
		items[i] = core.BulkItemResult{Index: i, Outcome: audit.OutcomeSucceeded}
		summary.Succeeded++
	}
	return core.BulkResult{Items: items, Summary: summary}
}

// Reassign sequences C9 reassign's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; C9 is in §5.6's destructive set).
func (s *Service) Reassign(
	ctx context.Context, caller core.Caller, moves []ReassignMove, confirm string, dryRun bool,
) (core.Result[ReassignPlan, core.BulkResult], error) {
	desc, _ := command.Lookup("C9")
	attempt := s.newEvent(caller, "C9", "")
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ReassignPlan, error) { return s.PlanReassign(ctx, moves) },
		func(plan ReassignPlan) (core.BulkResult, error) { return s.ApplyReassign(ctx, plan) },
	)
}

// CancelReassignments sequences C9 cancel's confirm/dryRun/audit lifecycle
// via core.Destructive (FUNC-SPEC §8.6, §9.1).
func (s *Service) CancelReassignments(
	ctx context.Context, caller core.Caller, targets []kafka.TopicPartition, confirm string, dryRun bool,
) (core.Result[ReassignPlan, core.BulkResult], error) {
	desc, _ := command.Lookup("C9")
	attempt := s.newEvent(caller, "C9", "")
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ReassignPlan, error) { return s.PlanCancelReassignments(ctx, targets) },
		func(plan ReassignPlan) (core.BulkResult, error) { return s.ApplyReassign(ctx, plan) },
	)
}

// ElectPlan is C9 elect's dry-run plan (FUNC-SPEC §8.6, V5). ConfirmTarget
// is the plan token over the sorted, de-duplicated topic-partition targets.
type ElectPlan struct {
	Preferred  bool
	Partitions []kafka.TopicPartition
	PlanToken  string
}

// ConfirmTarget implements core.Plan.
func (p ElectPlan) ConfirmTarget() string { return p.PlanToken }

// PlanElect resolves partitions' plan token, sorted for a stable response
// (FUNC-SPEC §8.6 C9 elect).
func (s *Service) PlanElect(ctx context.Context, preferred bool, partitions []kafka.TopicPartition) (ElectPlan, error) {
	sorted := append([]kafka.TopicPartition(nil), partitions...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Topic != sorted[j].Topic {
			return sorted[i].Topic < sorted[j].Topic
		}
		return sorted[i].Partition < sorted[j].Partition
	})
	targets := make([]string, len(sorted))
	for i, tp := range sorted {
		targets[i] = reassignTarget(tp.Topic, tp.Partition)
	}
	return ElectPlan{Preferred: preferred, Partitions: sorted, PlanToken: core.Token("C9", targets)}, nil
}

// ApplyElect triggers plan's election, converting each partition's own
// result into a bulk item (FUNC-SPEC §8.7 C9 elect).
func (s *Service) ApplyElect(ctx context.Context, plan ElectPlan) (core.BulkResult, error) {
	results, err := s.admin.ElectLeaders(ctx, plan.Preferred, plan.Partitions)
	if err != nil {
		return core.BulkResult{}, err
	}
	items := make([]core.BulkItemResult, len(results))
	summary := core.BulkSummary{Total: len(results)}
	for i, r := range results {
		if r.Err != nil {
			items[i] = core.BulkItemResult{Index: i, Outcome: audit.OutcomeFailed, Error: r.Err.Error()}
			summary.Failed++
			continue
		}
		items[i] = core.BulkItemResult{Index: i, Outcome: audit.OutcomeSucceeded}
		summary.Succeeded++
	}
	return core.BulkResult{Items: items, Summary: summary}, nil
}

// Elect sequences C9 elect's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1).
func (s *Service) Elect(
	ctx context.Context, caller core.Caller, preferred bool, partitions []kafka.TopicPartition, confirm string, dryRun bool,
) (core.Result[ElectPlan, core.BulkResult], error) {
	desc, _ := command.Lookup("C9")
	attempt := s.newEvent(caller, "C9", "")
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ElectPlan, error) { return s.PlanElect(ctx, preferred, partitions) },
		func(plan ElectPlan) (core.BulkResult, error) { return s.ApplyElect(ctx, plan) },
	)
}

// ImportTopic is one topic's desired definition (FUNC-SPEC §8.7 C12) — the
// same shape Export produces (FUNC-SPEC §8.6 C12's "topics: [C11 shape]").
type ImportTopic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// ImportAlterAction is one existing topic whose configs differ from its
// desired definition (FUNC-SPEC §8.6 C12 "alter").
type ImportAlterAction struct {
	Name    string
	Changes []ConfigChangeDetail
}

// ImportPlan is C12's dry-run plan (FUNC-SPEC §8.6, V5). ConfirmTarget is
// the plan token, built over each desired topic's name and a hash of its
// full definition (TECH-SPEC C8) — not the reconciled create/alter/delete
// split, so a change to only the caller's *submitted* desired set (not a
// change nobody asked for on the cluster) invalidates a stale token.
type ImportPlan struct {
	Create      []ImportTopic
	Alter       []ImportAlterAction
	Delete      []string
	Unchanged   []string
	AllowDelete bool
	PlanToken   string
}

// ConfirmTarget implements core.Plan.
func (p ImportPlan) ConfirmTarget() string { return p.PlanToken }

// hashDefinition returns a deterministic sha256 hex digest of one desired
// topic's full definition — partitions, replication factor, and configs
// sorted by key (TECH-SPEC C8: C12's plan token targets are
// "name=sha256(canonical desired definition)", not bare names).
func hashDefinition(t ImportTopic) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n%d\n", t.Partitions, t.ReplicationFactor)
	keys := make([]string, 0, len(t.Configs))
	for k := range t.Configs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, t.Configs[k])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// PlanImport reconciles desired against the cluster's current (non-internal)
// topics (FUNC-SPEC §8.6, §8.7 C12): a desired topic absent from the
// cluster is a create; present with differing configs is an alter, naming
// only the differing keys; present with matching configs is unchanged. An
// existing topic absent from desired is a delete candidate regardless of
// allowDelete — allowDelete only governs whether Apply actually deletes it
// or reports it skipped (FUNC-SPEC §8.6 C12 "allowDelete=false → deletes
// reported as skipped").
func (s *Service) PlanImport(ctx context.Context, desired []ImportTopic, allowDelete bool) (ImportPlan, error) {
	existing, err := s.admin.ListTopics(ctx)
	if err != nil {
		return ImportPlan{}, err
	}
	existingByName := make(map[string]bool, len(existing))
	for _, t := range existing {
		if !t.Internal {
			existingByName[t.Name] = true
		}
	}
	desiredByName := make(map[string]bool, len(desired))
	for _, t := range desired {
		desiredByName[t.Name] = true
	}

	var create []ImportTopic
	var alter []ImportAlterAction
	var unchanged []string
	for _, dt := range desired {
		if !existingByName[dt.Name] {
			create = append(create, dt)
			continue
		}
		configs, err := s.admin.DescribeTopicConfigs(ctx, dt.Name)
		if err != nil {
			return ImportPlan{}, err
		}
		byName := make(map[string]kafka.ConfigEntry, len(configs))
		for _, c := range configs {
			byName[c.Name] = c
		}
		var changes []ConfigChangeDetail
		for k, v := range dt.Configs {
			if byName[k].Value != v {
				changes = append(changes, ConfigChangeDetail{Name: k, From: byName[k].Value, To: v})
			}
		}
		if len(changes) == 0 {
			unchanged = append(unchanged, dt.Name)
			continue
		}
		sort.Slice(changes, func(i, j int) bool { return changes[i].Name < changes[j].Name })
		alter = append(alter, ImportAlterAction{Name: dt.Name, Changes: changes})
	}
	sort.Slice(create, func(i, j int) bool { return create[i].Name < create[j].Name })
	sort.Slice(alter, func(i, j int) bool { return alter[i].Name < alter[j].Name })
	sort.Strings(unchanged)

	var del []string
	for name := range existingByName {
		if !desiredByName[name] {
			del = append(del, name)
		}
	}
	sort.Strings(del)

	targets := make([]string, len(desired))
	for i, t := range desired {
		targets[i] = t.Name + "=" + hashDefinition(t)
	}

	return ImportPlan{
		Create: create, Alter: alter, Delete: del, Unchanged: unchanged, AllowDelete: allowDelete,
		PlanToken: core.Token("C12", targets),
	}, nil
}

// ImportItemResult is one topic's outcome of ApplyImport (FUNC-SPEC §8.7
// C12): Action is "create", "alter", "delete", or "skipped" (a delete
// candidate when AllowDelete is false).
type ImportItemResult struct {
	Name    string
	Action  string
	Outcome audit.Outcome
	Error   string
}

// ImportResult is ApplyImport's output. Summary counts only attempted
// actions (create, alter, a real delete) — a skipped delete candidate
// contributes to neither Succeeded nor Failed, since nothing was attempted
// for it; every item, skipped or not, still appears in Items.
type ImportResult struct {
	Items   []ImportItemResult
	Summary core.BulkSummary
}

// ApplyImport executes plan's reconciliation: every create, then every
// alter, then every delete (real when plan.AllowDelete, else reported
// skipped) — one item's failure never stops the rest (FUNC-SPEC §9.4).
func (s *Service) ApplyImport(ctx context.Context, plan ImportPlan) (ImportResult, error) {
	var items []ImportItemResult
	var summary core.BulkSummary

	for _, t := range plan.Create {
		spec := kafka.TopicSpec{Name: t.Name, Partitions: t.Partitions, ReplicationFactor: t.ReplicationFactor, Configs: t.Configs}
		results, err := s.admin.CreateTopics(ctx, []kafka.TopicSpec{spec}, false)
		summary.Total++
		if itemErr := firstErr(err, results); itemErr != nil {
			items = append(items, ImportItemResult{Name: t.Name, Action: "create", Outcome: audit.OutcomeFailed, Error: itemErr.Error()})
			summary.Failed++
			continue
		}
		items = append(items, ImportItemResult{Name: t.Name, Action: "create", Outcome: audit.OutcomeSucceeded})
		summary.Succeeded++
	}

	for _, a := range plan.Alter {
		changes := make([]kafka.ConfigChange, len(a.Changes))
		for i, c := range a.Changes {
			v := c.To
			changes[i] = kafka.ConfigChange{Name: c.Name, Value: &v}
		}
		err := s.admin.IncrementalAlterTopicConfigs(ctx, a.Name, changes)
		summary.Total++
		if err != nil {
			items = append(items, ImportItemResult{Name: a.Name, Action: "alter", Outcome: audit.OutcomeFailed, Error: err.Error()})
			summary.Failed++
			continue
		}
		items = append(items, ImportItemResult{Name: a.Name, Action: "alter", Outcome: audit.OutcomeSucceeded})
		summary.Succeeded++
	}

	for _, name := range plan.Delete {
		if !plan.AllowDelete {
			items = append(items, ImportItemResult{Name: name, Action: "skipped"})
			continue
		}
		results, err := s.admin.DeleteTopics(ctx, []string{name})
		summary.Total++
		if itemErr := firstDeleteErr(err, results); itemErr != nil {
			items = append(items, ImportItemResult{Name: name, Action: "delete", Outcome: audit.OutcomeFailed, Error: itemErr.Error()})
			summary.Failed++
			continue
		}
		items = append(items, ImportItemResult{Name: name, Action: "delete", Outcome: audit.OutcomeSucceeded})
		summary.Succeeded++
	}

	return ImportResult{Items: items, Summary: summary}, nil
}

// firstErr returns callErr, or else the one create result's own error, when
// either is set.
func firstErr(callErr error, results []kafka.TopicCreateResult) error {
	if callErr != nil {
		return callErr
	}
	if len(results) > 0 {
		return results[0].Err
	}
	return nil
}

// firstDeleteErr is firstErr for a DeleteTopics call.
func firstDeleteErr(callErr error, results []kafka.TopicDeleteResult) error {
	if callErr != nil {
		return callErr
	}
	if len(results) > 0 {
		return results[0].Err
	}
	return nil
}

// Import sequences C12's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1; C12 is in §5.6's destructive set when delete is
// enabled — it is routed through core.Destructive unconditionally here,
// since confirm/dryRun/audit apply the same way whether or not this
// particular call's plan includes any delete).
func (s *Service) Import(
	ctx context.Context, caller core.Caller, desired []ImportTopic, allowDelete bool, confirm string, dryRun bool,
) (core.Result[ImportPlan, ImportResult], error) {
	desc, _ := command.Lookup("C12")
	attempt := s.newEvent(caller, "C12", "")
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ImportPlan, error) { return s.PlanImport(ctx, desired, allowDelete) },
		func(plan ImportPlan) (ImportResult, error) { return s.ApplyImport(ctx, plan) },
	)
}
