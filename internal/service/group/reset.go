package group

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Reset mode values (FUNC-SPEC §8.7 G4).
const (
	ModeEarliest  = "earliest"
	ModeLatest    = "latest"
	ModeOffset    = "offset"
	ModeTimestamp = "timestamp"
)

// ResetTarget is G4's `target` request field (FUNC-SPEC §8.7 G4). Offsets,
// when non-empty, names the exact per-partition value to reset to directly
// and overrides Mode entirely for those partitions; otherwise Mode (plus
// Offset or TimestampMs) computes every scoped partition's new value.
type ResetTarget struct {
	Mode        string
	Offset      int64
	TimestampMs int64
	Offsets     map[kafka.TopicPartition]int64
}

// ResetOffsetDetail is one partition's offset move: Before/After render as
// plan.offsets[].current/target in a dry-run (FUNC-SPEC §8.6), and as
// result.offsets[].before/after on a real reset (FUNC-SPEC §8.7 G4) — the
// same computed values, named differently by the API layer's two DTOs.
type ResetOffsetDetail struct {
	Topic     string
	Partition int32
	Before    int64
	After     int64
}

// ResetPlan is G4's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is the
// group id.
type ResetPlan struct {
	GroupID string
	Offsets []ResetOffsetDetail
}

// ConfirmTarget implements core.Plan.
func (p ResetPlan) ConfirmTarget() string { return p.GroupID }

// ResetResult is ApplyReset's output (FUNC-SPEC §8.7 G4).
type ResetResult struct {
	GroupID string
	Offsets []ResetOffsetDetail
}

// PlanReset resolves target's scope and per-partition new values for
// groupID (FUNC-SPEC §8.6 G4): the group must have no active members, else
// *kafka.Error{Kind: KindGroupActive}; an absent group is allowed through
// (pre-seed) as long as topics or an explicit offsets map gives it
// something to reset.
func (s *Service) PlanReset(ctx context.Context, groupID string, target ResetTarget, topics []string) (ResetPlan, error) {
	exists, memberCount, err := s.groupState(ctx, groupID)
	if err != nil {
		return ResetPlan{}, err
	}
	if memberCount > 0 {
		return ResetPlan{}, &kafka.Error{Kind: kafka.KindGroupActive, Resource: "group"}
	}

	var committed map[kafka.TopicPartition]int64
	if exists {
		committed, err = s.admin.FetchGroupOffsets(ctx, groupID)
		if err != nil {
			return ResetPlan{}, err
		}
	}

	tps, err := s.resolveResetScope(ctx, target, topics, committed)
	if err != nil {
		return ResetPlan{}, err
	}

	byTopic := map[string][]int32{}
	for _, tp := range tps {
		byTopic[tp.Topic] = append(byTopic[tp.Topic], tp.Partition)
	}

	after := map[kafka.TopicPartition]int64{}
	for topicName := range byTopic {
		resolved, err := s.resolveResetTargetsForTopic(ctx, topicName, target)
		if err != nil {
			return ResetPlan{}, err
		}
		for p, v := range resolved {
			after[kafka.TopicPartition{Topic: topicName, Partition: p}] = v
		}
	}

	details := make([]ResetOffsetDetail, len(tps))
	for i, tp := range tps {
		v, ok := after[tp]
		if !ok {
			v = target.Offsets[tp]
		}
		details[i] = ResetOffsetDetail{Topic: tp.Topic, Partition: tp.Partition, Before: committed[tp], After: v}
	}
	sort.Slice(details, func(i, j int) bool {
		if details[i].Topic != details[j].Topic {
			return details[i].Topic < details[j].Topic
		}
		return details[i].Partition < details[j].Partition
	})

	return ResetPlan{GroupID: groupID, Offsets: details}, nil
}

// resolveResetScope resolves which topic-partitions PlanReset acts on: an
// explicit offsets map names them directly; otherwise topics (every
// partition of each named topic); otherwise, for a group that already has
// committed offsets, every topic-partition it has committed to (a bare
// "reset this group" with no topics named). None of the three → 400
// core.Validation: there is nothing to reset.
func (s *Service) resolveResetScope(ctx context.Context, target ResetTarget, topics []string, committed map[kafka.TopicPartition]int64) ([]kafka.TopicPartition, error) {
	if len(target.Offsets) > 0 {
		tps := make([]kafka.TopicPartition, 0, len(target.Offsets))
		for tp := range target.Offsets {
			tps = append(tps, tp)
		}
		return tps, nil
	}
	if len(topics) > 0 {
		return s.resolveScopeFromTopics(ctx, topics)
	}
	if len(committed) > 0 {
		tps := make([]kafka.TopicPartition, 0, len(committed))
		for tp := range committed {
			tps = append(tps, tp)
		}
		return tps, nil
	}
	return nil, &core.PolicyError{Code: core.Validation, Message: "topics or an explicit offsets map is required to reset a group with no committed offsets"}
}

// resolveScopeFromTopics expands topics into every one of their current
// partitions.
func (s *Service) resolveScopeFromTopics(ctx context.Context, topics []string) ([]kafka.TopicPartition, error) {
	var tps []kafka.TopicPartition
	for _, topic := range topics {
		end, err := s.admin.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		for p := range end {
			tps = append(tps, kafka.TopicPartition{Topic: topic, Partition: p})
		}
	}
	return tps, nil
}

// resolveResetTargetsForTopic computes topic's new offset per partition for
// target.Mode (FUNC-SPEC §8.7 G4, C10 timestamp clamping — already
// implemented by Admin.ListOffsetsAfterMilli itself). Returns nil when
// target carries an explicit offsets map: PlanReset falls back to reading
// those values directly for the partitions they name.
func (s *Service) resolveResetTargetsForTopic(ctx context.Context, topic string, target ResetTarget) (map[int32]int64, error) {
	if len(target.Offsets) > 0 {
		return nil, nil
	}
	switch target.Mode {
	case ModeEarliest:
		return s.admin.ListStartOffsets(ctx, topic)
	case ModeLatest:
		return s.admin.ListEndOffsets(ctx, topic)
	case ModeOffset:
		end, err := s.admin.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		out := make(map[int32]int64, len(end))
		for p := range end {
			out[p] = target.Offset
		}
		return out, nil
	case ModeTimestamp:
		return s.admin.ListOffsetsAfterMilli(ctx, topic, target.TimestampMs)
	default:
		return nil, &core.PolicyError{Code: core.Validation, Message: "target.mode must be one of earliest, latest, offset, timestamp"}
	}
}

// ApplyReset commits plan's resolved offsets to plan.GroupID (FUNC-SPEC
// §8.7 G4).
func (s *Service) ApplyReset(ctx context.Context, plan ResetPlan) (ResetResult, error) {
	offsets := make(map[kafka.TopicPartition]int64, len(plan.Offsets))
	for _, d := range plan.Offsets {
		offsets[kafka.TopicPartition{Topic: d.Topic, Partition: d.Partition}] = d.After
	}
	if err := s.admin.CommitGroupOffsets(ctx, plan.GroupID, offsets); err != nil {
		return ResetResult{}, err
	}
	return ResetResult{GroupID: plan.GroupID, Offsets: plan.Offsets}, nil
}

// Reset sequences G4's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1; G4 is in §5.6's destructive set).
func (s *Service) Reset(
	ctx context.Context, caller core.Caller, groupID string, target ResetTarget, topics []string, confirm string, dryRun bool,
) (core.Result[ResetPlan, ResetResult], error) {
	desc, _ := command.Lookup("G4")
	attempt := s.newEvent(caller, "G4", groupID)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ResetPlan, error) { return s.PlanReset(ctx, groupID, target, topics) },
		func(plan ResetPlan) (ResetResult, error) { return s.ApplyReset(ctx, plan) },
	)
}
