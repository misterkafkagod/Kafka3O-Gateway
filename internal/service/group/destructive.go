package group

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// DeleteGroupPlan is G5's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the group id.
type DeleteGroupPlan struct {
	GroupID          string
	CommittedOffsets int
}

// ConfirmTarget implements core.Plan.
func (p DeleteGroupPlan) ConfirmTarget() string { return p.GroupID }

// DeleteGroupResult is ApplyDeleteGroup's output (FUNC-SPEC §8.7 G5).
type DeleteGroupResult struct {
	Deleted string
}

// PlanDeleteGroup resolves groupID's committed-offset count for G5's plan
// (FUNC-SPEC §8.6): the group must exist and have no active members, else
// NotFound or *kafka.Error{Kind: KindGroupActive}.
func (s *Service) PlanDeleteGroup(ctx context.Context, groupID string) (DeleteGroupPlan, error) {
	exists, memberCount, err := s.groupState(ctx, groupID)
	if err != nil {
		return DeleteGroupPlan{}, err
	}
	if !exists {
		return DeleteGroupPlan{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "group"}
	}
	if memberCount > 0 {
		return DeleteGroupPlan{}, &kafka.Error{Kind: kafka.KindGroupActive, Resource: "group"}
	}

	committed, err := s.admin.FetchGroupOffsets(ctx, groupID)
	if err != nil {
		return DeleteGroupPlan{}, err
	}
	return DeleteGroupPlan{GroupID: groupID, CommittedOffsets: len(committed)}, nil
}

// ApplyDeleteGroup deletes plan.GroupID (FUNC-SPEC §8.7 G5).
func (s *Service) ApplyDeleteGroup(ctx context.Context, plan DeleteGroupPlan) (DeleteGroupResult, error) {
	results, err := s.admin.DeleteGroups(ctx, []string{plan.GroupID})
	if err != nil {
		return DeleteGroupResult{}, err
	}
	if results[0].Err != nil {
		return DeleteGroupResult{}, results[0].Err
	}
	return DeleteGroupResult{Deleted: plan.GroupID}, nil
}

// Delete sequences G5's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1; G5 is in §5.6's destructive set). Its successful
// RESULT is HIGH severity (FUNC-SPEC V6, audit.SeverityFor).
func (s *Service) Delete(
	ctx context.Context, caller core.Caller, groupID, confirm string, dryRun bool,
) (core.Result[DeleteGroupPlan, DeleteGroupResult], error) {
	desc, _ := command.Lookup("G5")
	attempt := s.newEvent(caller, "G5", groupID)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (DeleteGroupPlan, error) { return s.PlanDeleteGroup(ctx, groupID) },
		func(plan DeleteGroupPlan) (DeleteGroupResult, error) { return s.ApplyDeleteGroup(ctx, plan) },
	)
}

// RemoveMembersPlan is G6's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the group id.
type RemoveMembersPlan struct {
	GroupID string
	Members []string
}

// ConfirmTarget implements core.Plan.
func (p RemoveMembersPlan) ConfirmTarget() string { return p.GroupID }

// RemoveMembersResult is ApplyRemoveMembers's output (FUNC-SPEC §8.7 G6).
type RemoveMembersResult struct {
	Removed []string
}

// PlanRemoveMembers resolves G6's target member list (FUNC-SPEC §8.6):
// members as given, or — when empty, meaning "all" (FUNC-SPEC §8.7 G6) —
// every member groupID currently has. Unlike G4/G5, G6 has no "no active
// members" precondition: evicting members is exactly how an operator
// escapes a stuck active group.
func (s *Service) PlanRemoveMembers(ctx context.Context, groupID string, members []string) (RemoveMembersPlan, error) {
	if len(members) > 0 {
		return RemoveMembersPlan{GroupID: groupID, Members: members}, nil
	}
	groups, err := s.admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return RemoveMembersPlan{}, err
	}
	all := make([]string, len(groups[0].Members))
	for i, m := range groups[0].Members {
		all[i] = m.MemberID
	}
	sort.Strings(all)
	return RemoveMembersPlan{GroupID: groupID, Members: all}, nil
}

// ApplyRemoveMembers evicts plan.Members from plan.GroupID (FUNC-SPEC §8.7
// G6). One member's own error never fails the rest — Removed lists only
// the members that actually left.
func (s *Service) ApplyRemoveMembers(ctx context.Context, plan RemoveMembersPlan) (RemoveMembersResult, error) {
	results, err := s.admin.LeaveGroup(ctx, plan.GroupID, plan.Members)
	if err != nil {
		return RemoveMembersResult{}, err
	}
	removed := make([]string, 0, len(results))
	for _, r := range results {
		if r.Err == nil {
			removed = append(removed, r.MemberID)
		}
	}
	return RemoveMembersResult{Removed: removed}, nil
}

// RemoveMembers sequences G6's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; G6 is in §5.6's destructive set).
func (s *Service) RemoveMembers(
	ctx context.Context, caller core.Caller, groupID string, members []string, confirm string, dryRun bool,
) (core.Result[RemoveMembersPlan, RemoveMembersResult], error) {
	desc, _ := command.Lookup("G6")
	attempt := s.newEvent(caller, "G6", groupID)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (RemoveMembersPlan, error) { return s.PlanRemoveMembers(ctx, groupID, members) },
		func(plan RemoveMembersPlan) (RemoveMembersResult, error) { return s.ApplyRemoveMembers(ctx, plan) },
	)
}

// CloneOffsetDetail is one partition's clone (FUNC-SPEC §8.6, §8.7 G7).
type CloneOffsetDetail struct {
	Topic         string
	Partition     int32
	TargetCurrent int64
	NewValue      int64
}

// CloneOffsetsPlan is G7's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the target group id — never the source (FUNC-SPEC §8.6 G7 row: "target
// group id").
type CloneOffsetsPlan struct {
	Target  string
	Offsets []CloneOffsetDetail
}

// ConfirmTarget implements core.Plan.
func (p CloneOffsetsPlan) ConfirmTarget() string { return p.Target }

// CloneOffsetsResult is ApplyCloneOffsets's output (FUNC-SPEC §8.7 G7).
type CloneOffsetsResult struct {
	Target  string
	Offsets []CloneOffsetDetail
}

// PlanCloneOffsets resolves source's committed offsets (optionally scoped
// to topics) as the values to copy onto target (FUNC-SPEC §8.6 G7): target
// must have no active members, else *kafka.Error{Kind: KindGroupActive}
// ("target inactive, else 409").
func (s *Service) PlanCloneOffsets(ctx context.Context, target, source string, topics []string) (CloneOffsetsPlan, error) {
	exists, memberCount, err := s.groupState(ctx, target)
	if err != nil {
		return CloneOffsetsPlan{}, err
	}
	if memberCount > 0 {
		return CloneOffsetsPlan{}, &kafka.Error{Kind: kafka.KindGroupActive, Resource: "group"}
	}

	var targetCommitted map[kafka.TopicPartition]int64
	if exists {
		targetCommitted, err = s.admin.FetchGroupOffsets(ctx, target)
		if err != nil {
			return CloneOffsetsPlan{}, err
		}
	}

	sourceCommitted, err := s.admin.FetchGroupOffsets(ctx, source)
	if err != nil {
		return CloneOffsetsPlan{}, err
	}

	var topicSet map[string]bool
	if len(topics) > 0 {
		topicSet = make(map[string]bool, len(topics))
		for _, t := range topics {
			topicSet[t] = true
		}
	}

	tps := make([]kafka.TopicPartition, 0, len(sourceCommitted))
	for tp := range sourceCommitted {
		if topicSet != nil && !topicSet[tp.Topic] {
			continue
		}
		tps = append(tps, tp)
	}
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].Topic != tps[j].Topic {
			return tps[i].Topic < tps[j].Topic
		}
		return tps[i].Partition < tps[j].Partition
	})

	details := make([]CloneOffsetDetail, len(tps))
	for i, tp := range tps {
		details[i] = CloneOffsetDetail{
			Topic: tp.Topic, Partition: tp.Partition,
			TargetCurrent: targetCommitted[tp], NewValue: sourceCommitted[tp],
		}
	}
	return CloneOffsetsPlan{Target: target, Offsets: details}, nil
}

// ApplyCloneOffsets commits plan's resolved offsets to plan.Target
// (FUNC-SPEC §8.7 G7).
func (s *Service) ApplyCloneOffsets(ctx context.Context, plan CloneOffsetsPlan) (CloneOffsetsResult, error) {
	offsets := make(map[kafka.TopicPartition]int64, len(plan.Offsets))
	for _, d := range plan.Offsets {
		offsets[kafka.TopicPartition{Topic: d.Topic, Partition: d.Partition}] = d.NewValue
	}
	if err := s.admin.CommitGroupOffsets(ctx, plan.Target, offsets); err != nil {
		return CloneOffsetsResult{}, err
	}
	return CloneOffsetsResult{Target: plan.Target, Offsets: plan.Offsets}, nil
}

// CloneOffsets sequences G7's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; G7 is in §5.6's destructive set).
func (s *Service) CloneOffsets(
	ctx context.Context, caller core.Caller, target, source string, topics []string, confirm string, dryRun bool,
) (core.Result[CloneOffsetsPlan, CloneOffsetsResult], error) {
	desc, _ := command.Lookup("G7")
	attempt := s.newEvent(caller, "G7", target)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (CloneOffsetsPlan, error) { return s.PlanCloneOffsets(ctx, target, source, topics) },
		func(plan CloneOffsetsPlan) (CloneOffsetsResult, error) { return s.ApplyCloneOffsets(ctx, plan) },
	)
}
