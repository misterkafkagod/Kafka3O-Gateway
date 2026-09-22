package topic

import (
	"context"
	"regexp"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// DeleteTopicPlan is T7's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the topic name.
type DeleteTopicPlan struct {
	Topic          string
	Partitions     int32
	ApproxMessages int64
}

// ConfirmTarget implements core.Plan.
func (p DeleteTopicPlan) ConfirmTarget() string { return p.Topic }

// DeleteTopicResult is ApplyDelete's output (FUNC-SPEC §8.7 T7).
type DeleteTopicResult struct {
	Deleted string
}

// PlanDelete resolves topicName's partition count and approximate message
// count for T7's plan (FUNC-SPEC §8.6). Unknown topic → the port's NotFound
// error, unchanged (§8.6's "topic exists, else 404").
func (s *Service) PlanDelete(ctx context.Context, topicName string) (DeleteTopicPlan, error) {
	d, err := s.Describe(ctx, topicName)
	if err != nil {
		return DeleteTopicPlan{}, err
	}
	return DeleteTopicPlan{Topic: d.Name, Partitions: int32(len(d.Partitions)), ApproxMessages: d.ApproxMessageCount}, nil
}

// ApplyDelete deletes plan.Topic (FUNC-SPEC §8.7 T7).
func (s *Service) ApplyDelete(ctx context.Context, plan DeleteTopicPlan) (DeleteTopicResult, error) {
	results, err := s.admin.DeleteTopics(ctx, []string{plan.Topic})
	if err != nil {
		return DeleteTopicResult{}, err
	}
	if results[0].Err != nil {
		return DeleteTopicResult{}, results[0].Err
	}
	return DeleteTopicResult{Deleted: plan.Topic}, nil
}

// Delete sequences T7's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1; T7 is in §5.6's destructive set). Its successful
// RESULT is HIGH severity (FUNC-SPEC V6, audit.SeverityFor) — irreversible
// data loss.
func (s *Service) Delete(
	ctx context.Context, caller core.Caller, topicName, confirm string, dryRun bool,
) (core.Result[DeleteTopicPlan, DeleteTopicResult], error) {
	desc, _ := command.Lookup("T7")
	attempt := s.newEvent(caller, "T7", topicName)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (DeleteTopicPlan, error) { return s.PlanDelete(ctx, topicName) },
		func(plan DeleteTopicPlan) (DeleteTopicResult, error) { return s.ApplyDelete(ctx, plan) },
	)
}

// BulkDeletePlan is T8's dry-run plan (FUNC-SPEC §8.6): Topics is the
// resolved, sorted, de-duplicated target list; ConfirmTarget is the plan
// token built from it (core.Token, FUNC-SPEC V5) — the execute call
// re-resolves the same list or pattern, so a target set that has since
// changed yields a different token and a fresh 400 CONFIRMATION_MISMATCH
// carrying the fresh plan (core.Destructive already attaches it).
type BulkDeletePlan struct {
	Topics    []string
	PlanToken string
}

// ConfirmTarget implements core.Plan.
func (p BulkDeletePlan) ConfirmTarget() string { return p.PlanToken }

// resolveBulkDeleteTargets resolves T8's target topics: by pattern (an RE2
// regular expression matched unanchored against every existing topic's
// name, FUNC-SPEC §8.2 `?pattern=`) when pattern is non-empty, else the
// caller's own explicit list (a listed name need not currently exist — a
// missing one simply fails its own item at apply time, FUNC-SPEC §9.4).
// Either way the result is sorted and de-duplicated, matching the plan
// token's own canonical form.
func (s *Service) resolveBulkDeleteTargets(ctx context.Context, topics []string, pattern string) ([]string, error) {
	if pattern != "" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, &core.PolicyError{Code: core.InvalidRegex, Message: err.Error()}
		}
		all, err := s.admin.ListTopics(ctx)
		if err != nil {
			return nil, err
		}
		matched := make([]string, 0, len(all))
		for _, t := range all {
			if re.MatchString(t.Name) {
				matched = append(matched, t.Name)
			}
		}
		sort.Strings(matched)
		return matched, nil
	}

	if len(topics) == 0 {
		return nil, &core.PolicyError{Code: core.Validation, Message: "topics or pattern is required"}
	}
	seen := make(map[string]bool, len(topics))
	unique := make([]string, 0, len(topics))
	for _, t := range topics {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}
	sort.Strings(unique)
	return unique, nil
}

// PlanBulkDelete resolves T8's target topics and their plan token
// (FUNC-SPEC §8.6).
func (s *Service) PlanBulkDelete(ctx context.Context, topics []string, pattern string) (BulkDeletePlan, error) {
	resolved, err := s.resolveBulkDeleteTargets(ctx, topics, pattern)
	if err != nil {
		return BulkDeletePlan{}, err
	}
	return BulkDeletePlan{Topics: resolved, PlanToken: core.Token("T8", resolved)}, nil
}

// ApplyBulkDelete deletes every topic in plan.Topics in one port call,
// converting each one's own per-topic result into a bulk item (FUNC-SPEC
// §9.4 step 4: one topic's failure never fails the rest).
func (s *Service) ApplyBulkDelete(ctx context.Context, plan BulkDeletePlan) (core.BulkResult, error) {
	results, err := s.admin.DeleteTopics(ctx, plan.Topics)
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

// BulkDelete sequences T8's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; T8 is in §5.6's destructive set).
// Its successful RESULT is HIGH severity (FUNC-SPEC V6).
func (s *Service) BulkDelete(
	ctx context.Context, caller core.Caller, topics []string, pattern, confirm string, dryRun bool,
) (core.Result[BulkDeletePlan, core.BulkResult], error) {
	desc, _ := command.Lookup("T8")
	attempt := s.newEvent(caller, "T8", "")
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (BulkDeletePlan, error) { return s.PlanBulkDelete(ctx, topics, pattern) },
		func(plan BulkDeletePlan) (core.BulkResult, error) { return s.ApplyBulkDelete(ctx, plan) },
	)
}

// PartitionWatermark is one partition's new begin (low watermark) offset
// after a delete-records or purge apply (FUNC-SPEC §8.7 T11, T12).
type PartitionWatermark struct {
	ID           int32
	LowWatermark int64
}

// PartitionDeleteRecordsDetail is one partition's planned truncation
// (FUNC-SPEC §8.6 T11).
type PartitionDeleteRecordsDetail struct {
	Partition             int32
	BeginOffset           int64
	TruncateTo            int64
	ApproxRecordsAffected int64
}

// DeleteRecordsPlan is T11 and T12's dry-run plan (FUNC-SPEC §8.6).
// ConfirmTarget is the topic name.
type DeleteRecordsPlan struct {
	Topic      string
	Partitions []PartitionDeleteRecordsDetail
}

// ConfirmTarget implements core.Plan.
func (p DeleteRecordsPlan) ConfirmTarget() string { return p.Topic }

// DeleteRecordsResult is ApplyDeleteRecords's output (FUNC-SPEC §8.7 T11, T12).
type DeleteRecordsResult struct {
	Partitions []PartitionWatermark
}

// PlanDeleteRecords resolves each partition in offsets to a full plan
// detail (FUNC-SPEC §8.6 T11): an unknown partition or a truncateTo beyond
// its current end offset → 400 core.Validation (the manual test plan's
// `{"0":999}` case).
func (s *Service) PlanDeleteRecords(ctx context.Context, topicName string, offsets map[int32]int64) (DeleteRecordsPlan, error) {
	t, err := s.admin.DescribeTopics(ctx, topicName)
	if err != nil {
		return DeleteRecordsPlan{}, err
	}
	begin, err := s.admin.ListStartOffsets(ctx, topicName)
	if err != nil {
		return DeleteRecordsPlan{}, err
	}
	end, err := s.admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		return DeleteRecordsPlan{}, err
	}
	exists := make(map[int32]bool, len(t.Partitions))
	for _, p := range t.Partitions {
		exists[p.ID] = true
	}

	partitions := make([]PartitionDeleteRecordsDetail, 0, len(offsets))
	for partition, truncateTo := range offsets {
		if !exists[partition] {
			return DeleteRecordsPlan{}, &core.PolicyError{Code: core.Validation, Message: "unknown partition"}
		}
		if truncateTo > end[partition] {
			return DeleteRecordsPlan{}, &core.PolicyError{Code: core.Validation, Message: "truncateTo exceeds the partition's end offset"}
		}
		affected := truncateTo - begin[partition]
		if affected < 0 {
			affected = 0
		}
		partitions = append(partitions, PartitionDeleteRecordsDetail{
			Partition: partition, BeginOffset: begin[partition], TruncateTo: truncateTo, ApproxRecordsAffected: affected,
		})
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Partition < partitions[j].Partition })
	return DeleteRecordsPlan{Topic: topicName, Partitions: partitions}, nil
}

// PlanPurge builds T12's plan: T11's plan with every partition's truncateTo
// set to its current end offset (FUNC-SPEC §8.6 T12).
func (s *Service) PlanPurge(ctx context.Context, topicName string) (DeleteRecordsPlan, error) {
	t, err := s.admin.DescribeTopics(ctx, topicName)
	if err != nil {
		return DeleteRecordsPlan{}, err
	}
	end, err := s.admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		return DeleteRecordsPlan{}, err
	}
	offsets := make(map[int32]int64, len(t.Partitions))
	for _, p := range t.Partitions {
		offsets[p.ID] = end[p.ID]
	}
	return s.PlanDeleteRecords(ctx, topicName, offsets)
}

// ApplyDeleteRecords truncates plan.Topic's partitions to their planned
// truncateTo (FUNC-SPEC §8.7 T11, T12).
func (s *Service) ApplyDeleteRecords(ctx context.Context, plan DeleteRecordsPlan) (DeleteRecordsResult, error) {
	truncateTo := make(map[int32]int64, len(plan.Partitions))
	for _, p := range plan.Partitions {
		truncateTo[p.Partition] = p.TruncateTo
	}
	results, err := s.admin.DeleteRecords(ctx, plan.Topic, truncateTo)
	if err != nil {
		return DeleteRecordsResult{}, err
	}
	out := make([]PartitionWatermark, len(results))
	for i, r := range results {
		out[i] = PartitionWatermark{ID: r.Partition, LowWatermark: r.LowWatermark}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return DeleteRecordsResult{Partitions: out}, nil
}

// DeleteRecords sequences T11's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1). Its successful RESULT is HIGH
// severity (FUNC-SPEC V6) — irreversible data loss.
func (s *Service) DeleteRecords(
	ctx context.Context, caller core.Caller, topicName string, offsets map[int32]int64, confirm string, dryRun bool,
) (core.Result[DeleteRecordsPlan, DeleteRecordsResult], error) {
	desc, _ := command.Lookup("T11")
	attempt := s.newEvent(caller, "T11", topicName)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (DeleteRecordsPlan, error) { return s.PlanDeleteRecords(ctx, topicName, offsets) },
		func(plan DeleteRecordsPlan) (DeleteRecordsResult, error) { return s.ApplyDeleteRecords(ctx, plan) },
	)
}

// Purge sequences T12's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1). Its successful RESULT is HIGH severity (FUNC-SPEC
// V6) — irreversible data loss.
func (s *Service) Purge(
	ctx context.Context, caller core.Caller, topicName, confirm string, dryRun bool,
) (core.Result[DeleteRecordsPlan, DeleteRecordsResult], error) {
	desc, _ := command.Lookup("T12")
	attempt := s.newEvent(caller, "T12", topicName)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (DeleteRecordsPlan, error) { return s.PlanPurge(ctx, topicName) },
		func(plan DeleteRecordsPlan) (DeleteRecordsResult, error) { return s.ApplyDeleteRecords(ctx, plan) },
	)
}
