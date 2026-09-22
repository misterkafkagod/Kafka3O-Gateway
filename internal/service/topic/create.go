package topic

import (
	"context"
	"fmt"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// CreateParams is one topic to create (FUNC-SPEC §8.7 T5, T6).
type CreateParams struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

// CreateResult is Create's (and one CreateBulk item's) outcome (FUNC-SPEC
// §8.7 T5, T6).
type CreateResult struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           []kafka.ConfigEntry
}

// Create creates one topic — or, when dryRun, asks the broker to validate
// it without creating anything (FUNC-SPEC §8.7 T5, §8.2 `?dryRun=true`);
// T5 carries no `confirm` (it is not in FUNC-SPEC §5.6's destructive set),
// so it sequences its own ATTEMPT/RESULT directly rather than through
// core.Destructive. isDryRun tells the caller which envelope to render.
// An existing name → *kafka.Error{Kind: AlreadyExists}.
func (s *Service) Create(ctx context.Context, caller core.Caller, params CreateParams, dryRun bool) (result CreateResult, isDryRun bool, err error) {
	if err := s.checkGate(ctx, caller, "T5", params.Name); err != nil {
		return CreateResult{}, false, err
	}

	spec := kafka.TopicSpec{
		Name: params.Name, Partitions: params.Partitions, ReplicationFactor: params.ReplicationFactor, Configs: params.Configs,
	}

	if dryRun {
		created, err := s.createOne(ctx, spec, true)
		if err != nil {
			return CreateResult{}, false, err
		}
		attempt := s.newEvent(caller, "T5", params.Name)
		attempt.DryRun = true
		s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeSucceeded, ""))
		return created, true, nil
	}

	attempt := s.newEvent(caller, "T5", params.Name)
	if err := s.auditor.Attempt(ctx, attempt); err != nil {
		return CreateResult{}, false, &core.PolicyError{Code: core.AuditUnavailable, Message: err.Error()}
	}

	created, err := s.createOne(ctx, spec, false)
	if err != nil {
		s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeFailed, errCodeOf(err)))
		return CreateResult{}, false, err
	}
	s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeSucceeded, ""))
	return created, false, nil
}

// createOne calls Admin.CreateTopics for a single spec and unwraps its
// per-spec result into an error, matching how DescribeTopics and friends
// report a single-topic failure.
func (s *Service) createOne(ctx context.Context, spec kafka.TopicSpec, validateOnly bool) (CreateResult, error) {
	results, err := s.admin.CreateTopics(ctx, []kafka.TopicSpec{spec}, validateOnly)
	if err != nil {
		return CreateResult{}, err
	}
	if results[0].Err != nil {
		return CreateResult{}, results[0].Err
	}
	return toCreateResult(results[0]), nil
}

// CreateBulk creates every params entry, first validating none of their
// names already exist (FUNC-SPEC §9.4 V3: validate-all, nothing executes
// on any failure, no ATTEMPT) — one existing-topic check via
// Admin.ListTopics rather than one DescribeTopics per spec.
func (s *Service) CreateBulk(ctx context.Context, caller core.Caller, params []CreateParams) (core.BulkResult, error) {
	if err := s.checkGate(ctx, caller, "T6", ""); err != nil {
		return core.BulkResult{}, err
	}

	existing, err := s.admin.ListTopics(ctx)
	if err != nil {
		return core.BulkResult{}, err
	}
	existingNames := make(map[string]bool, len(existing))
	for _, t := range existing {
		existingNames[t.Name] = true
	}

	attempt := s.newEvent(caller, "T6", "")
	return core.BulkRun(ctx, s.auditor, attempt,
		func(summary core.BulkSummary) audit.Event { return s.resultEvent(attempt, summary) },
		params,
		func(p CreateParams) error {
			if existingNames[p.Name] {
				return fmt.Errorf("topic %q already exists", p.Name)
			}
			return nil
		},
		func(ctx context.Context, p CreateParams) error {
			spec := kafka.TopicSpec{Name: p.Name, Partitions: p.Partitions, ReplicationFactor: p.ReplicationFactor, Configs: p.Configs}
			results, err := s.admin.CreateTopics(ctx, []kafka.TopicSpec{spec}, false)
			if err != nil {
				return err
			}
			return results[0].Err
		},
	)
}

// toCreateResult converts the port's per-spec result into CreateResult.
func toCreateResult(r kafka.TopicCreateResult) CreateResult {
	return CreateResult{Name: r.Name, Partitions: r.Partitions, ReplicationFactor: r.ReplicationFactor, Configs: r.Configs}
}
