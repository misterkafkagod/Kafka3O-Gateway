package security

import (
	"context"
	"sort"
	"strings"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// DeletePlan is S1 delete's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is
// the user name.
type DeletePlan struct {
	User       string
	Mechanisms []kafka.ScramMechanism
}

// ConfirmTarget implements core.Plan.
func (p DeletePlan) ConfirmTarget() string { return p.User }

// DeleteResult is ApplyDelete's output (FUNC-SPEC §8.7 S1 delete).
type DeleteResult struct {
	Deleted string
}

// PlanDelete resolves user's current mechanisms for S1 delete's plan
// (FUNC-SPEC §8.6): mechanism, when non-empty, narrows the plan (and the
// eventual delete) to that one mechanism only; empty (FUNC-SPEC §8.7 S1
// delete "mechanism?") means every mechanism the user currently has.
// Unknown user, or a named mechanism the user doesn't have, →
// *kafka.Error{Kind: NotFound, Resource: "user"}.
func (s *Service) PlanDelete(ctx context.Context, user string, mechanism kafka.ScramMechanism) (DeletePlan, error) {
	users, err := s.admin.DescribeUserSCRAMs(ctx, user)
	if err != nil {
		return DeletePlan{}, err
	}
	all := users[0].Credentials

	var mechanisms []kafka.ScramMechanism
	if mechanism != "" {
		found := false
		for _, c := range all {
			if c.Mechanism == mechanism {
				found = true
			}
		}
		if !found {
			return DeletePlan{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "user"}
		}
		mechanisms = []kafka.ScramMechanism{mechanism}
	} else {
		for _, c := range all {
			mechanisms = append(mechanisms, c.Mechanism)
		}
		sort.Slice(mechanisms, func(i, j int) bool { return mechanisms[i] < mechanisms[j] })
	}
	return DeletePlan{User: user, Mechanisms: mechanisms}, nil
}

// ApplyDelete removes plan.Mechanisms from plan.User (FUNC-SPEC §8.7 S1 delete).
func (s *Service) ApplyDelete(ctx context.Context, plan DeletePlan) (DeleteResult, error) {
	deletes := make([]kafka.ScramDelete, len(plan.Mechanisms))
	for i, m := range plan.Mechanisms {
		deletes[i] = kafka.ScramDelete{User: plan.User, Mechanism: m}
	}
	results, err := s.admin.AlterUserSCRAMs(ctx, nil, deletes)
	if err != nil {
		return DeleteResult{}, err
	}
	for _, r := range results {
		if r.Err != nil {
			return DeleteResult{}, r.Err
		}
	}
	return DeleteResult{Deleted: plan.User}, nil
}

// Delete sequences S1 delete's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; S1 delete is in §5.6's destructive
// set — S1 create is not).
func (s *Service) Delete(
	ctx context.Context, caller core.Caller, user string, mechanism kafka.ScramMechanism, confirm string, dryRun bool,
) (core.Result[DeletePlan, DeleteResult], error) {
	desc, _ := command.Lookup("S1")
	attempt := s.newEvent(caller, "S1", user)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (DeletePlan, error) { return s.PlanDelete(ctx, user, mechanism) },
		func(plan DeletePlan) (DeleteResult, error) { return s.ApplyDelete(ctx, plan) },
	)
}

// entityDescriptor renders entity as its confirm target and audit
// Target.Name (FUNC-SPEC §8.6 S2: "entity descriptor, e.g. `user:alice`"):
// each component as `type:name` (name "" for a default match), components
// sorted by type so an entity with more than one component still renders
// deterministically.
func entityDescriptor(entity kafka.QuotaEntity) string {
	parts := make([]string, len(entity))
	for i, c := range entity {
		name := ""
		if c.Name != nil {
			name = *c.Name
		}
		parts[i] = c.Type + ":" + name
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// QuotaChangeDetail is one quota key's planned or applied change (FUNC-SPEC
// §8.6 S2). From is nil when the key was not previously configured; To is
// nil for a removal.
type QuotaChangeDetail struct {
	Key  string
	From *float64
	To   *float64
}

// AlterQuotaPlan is S2 alter's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget
// is the entity descriptor, e.g. "user:alice".
type AlterQuotaPlan struct {
	Entity  kafka.QuotaEntity
	Changes []QuotaChangeDetail
}

// ConfirmTarget implements core.Plan.
func (p AlterQuotaPlan) ConfirmTarget() string { return entityDescriptor(p.Entity) }

// AlterQuotaResult is ApplyAlterQuota's output (FUNC-SPEC §8.7 S2 alter).
type AlterQuotaResult struct {
	Entity kafka.QuotaEntity
	Values []kafka.QuotaValue
}

// currentQuotaValues returns entity's currently configured quota values,
// keyed by quota key. An entity with none configured yet reports an empty
// map, not an error (FUNC-SPEC §8.6 S2 has no existence precondition,
// unlike G4/G5/G7's active-group checks). DescribeClientQuotas only filters
// by entity type, not the full entity, so this narrows to entity's own
// type(s) when unambiguous and then matches the exact entity descriptor.
func (s *Service) currentQuotaValues(ctx context.Context, entity kafka.QuotaEntity) (map[string]float64, error) {
	types := map[string]bool{}
	for _, c := range entity {
		types[c.Type] = true
	}
	var entityType string
	if len(types) == 1 {
		for t := range types {
			entityType = t
		}
	}

	described, err := s.admin.DescribeClientQuotas(ctx, entityType)
	if err != nil {
		return nil, err
	}
	key := entityDescriptor(entity)
	for _, d := range described {
		if entityDescriptor(d.Entity) != key {
			continue
		}
		out := make(map[string]float64, len(d.Values))
		for _, v := range d.Values {
			out[v.Key] = v.Value
		}
		return out, nil
	}
	return map[string]float64{}, nil
}

// PlanAlterQuota resolves entity's current quota values for every key in
// set or remove, so the plan's changes[] can show from/to (FUNC-SPEC §8.6 S2).
func (s *Service) PlanAlterQuota(ctx context.Context, entity kafka.QuotaEntity, set map[string]float64, remove []string) (AlterQuotaPlan, error) {
	current, err := s.currentQuotaValues(ctx, entity)
	if err != nil {
		return AlterQuotaPlan{}, err
	}

	changes := make([]QuotaChangeDetail, 0, len(set)+len(remove))
	for key, value := range set {
		v := value
		var from *float64
		if f, ok := current[key]; ok {
			from = &f
		}
		changes = append(changes, QuotaChangeDetail{Key: key, From: from, To: &v})
	}
	for _, key := range remove {
		var from *float64
		if f, ok := current[key]; ok {
			from = &f
		}
		changes = append(changes, QuotaChangeDetail{Key: key, From: from})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Key < changes[j].Key })

	return AlterQuotaPlan{Entity: entity, Changes: changes}, nil
}

// ApplyAlterQuota applies set/remove to entity and re-describes it so the
// response's values carry the freshly configured quotas (FUNC-SPEC §8.7 S2 alter).
func (s *Service) ApplyAlterQuota(ctx context.Context, entity kafka.QuotaEntity, set map[string]float64, remove []string) (AlterQuotaResult, error) {
	ops := make([]kafka.QuotaOp, 0, len(set)+len(remove))
	for key, value := range set {
		ops = append(ops, kafka.QuotaOp{Key: key, Value: value})
	}
	for _, key := range remove {
		ops = append(ops, kafka.QuotaOp{Key: key, Remove: true})
	}

	results, err := s.admin.AlterClientQuotas(ctx, []kafka.QuotaAlterEntry{{Entity: entity, Ops: ops}})
	if err != nil {
		return AlterQuotaResult{}, err
	}
	if results[0].Err != nil {
		return AlterQuotaResult{}, results[0].Err
	}

	values, err := s.currentQuotaValues(ctx, entity)
	if err != nil {
		return AlterQuotaResult{}, err
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kafka.QuotaValue, len(keys))
	for i, k := range keys {
		out[i] = kafka.QuotaValue{Key: k, Value: values[k]}
	}
	return AlterQuotaResult{Entity: entity, Values: out}, nil
}

// AlterQuota sequences S2 alter's confirm/dryRun/audit lifecycle via
// core.Destructive (FUNC-SPEC §8.6, §9.1; S2 alter is in §5.6's destructive
// set — S2 list is not).
func (s *Service) AlterQuota(
	ctx context.Context, caller core.Caller, entity kafka.QuotaEntity, set map[string]float64, remove []string, confirm string, dryRun bool,
) (core.Result[AlterQuotaPlan, AlterQuotaResult], error) {
	desc, _ := command.Lookup("S2")
	attempt := s.newEvent(caller, "S2", entityDescriptor(entity))
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (AlterQuotaPlan, error) { return s.PlanAlterQuota(ctx, entity, set, remove) },
		func(AlterQuotaPlan) (AlterQuotaResult, error) { return s.ApplyAlterQuota(ctx, entity, set, remove) },
	)
}
