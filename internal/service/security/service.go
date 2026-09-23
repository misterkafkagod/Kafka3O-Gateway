// Package security implements the SCRAM credential and client-quota
// commands (FUNC-SPEC §8.7 S1, S2). Service holds only kafka.Admin
// (TECH-SPEC I2, S2).
package security

import (
	"context"
	"sort"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// DefaultIterations is S1 create's SCRAM iteration count when the caller
// omits one (FUNC-SPEC §8.7 S1 create "iterations?"; TECH-SPEC C11).
const DefaultIterations int32 = 4096

// Service implements the security commands over a kafka.Admin.
type Service struct {
	admin kafka.Admin
	// auditor records S1 (create, delete) and S2 (alter)'s ATTEMPT/RESULT
	// audit trail (FUNC-SPEC §8.5) — S1 list and S2 list are reads and stay
	// unaudited (FUNC-SPEC §8.5 scope).
	auditor    *audit.Auditor
	newEventID func() string
	now        func() time.Time
	// runner gates S1 and S2 (FUNC-SPEC §9.1 nodes F-K3): an operator-only
	// caller, plus F3's per-operation switch.
	runner core.Runner
}

// New builds a Service over admin, auditor, and runner.
func New(admin kafka.Admin, auditor *audit.Auditor, runner core.Runner) *Service {
	return &Service{admin: admin, auditor: auditor, newEventID: audit.NewEventID, now: time.Now, runner: runner}
}

// UserItem is one user in a ListUsers result (FUNC-SPEC §8.7 S1 list). No
// secret material ever appears here (TECH-SPEC C7).
type UserItem struct {
	Name        string
	Credentials []kafka.ScramCredential
}

// ListUsers returns every SCRAM user with credentials configured, sorted by
// name for a stable response (FUNC-SPEC §8.7 S1 list).
func (s *Service) ListUsers(ctx context.Context) ([]UserItem, error) {
	users, err := s.admin.DescribeUserSCRAMs(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	out := make([]UserItem, len(users))
	for i, u := range users {
		out[i] = UserItem{Name: u.Name, Credentials: u.Credentials}
	}
	return out, nil
}

// CreateParams is one SCRAM user credential to create (FUNC-SPEC §8.7 S1
// create). Iterations of 0 means "use DefaultIterations".
type CreateParams struct {
	Name       string
	Mechanism  kafka.ScramMechanism
	Password   string
	Iterations int32
}

// CreateResult is Create's outcome (FUNC-SPEC §8.7 S1 create). It carries no
// password (FUNC-SPEC §8.2: never echoed back).
type CreateResult struct {
	Name      string
	Mechanism kafka.ScramMechanism
}

// Create creates one SCRAM user's password for one mechanism (FUNC-SPEC
// §8.7 S1 create). S1 create carries no `confirm` (it is not in FUNC-SPEC
// §5.6's destructive set — only S1 delete is), so it sequences its own
// ATTEMPT/RESULT directly rather than through core.Destructive, the same
// shape as topic.Service.Create (T5). The password itself never reaches the
// audit event: newEvent's Target carries only the username (FUNC-SPEC §8.2,
// TECH-SPEC C7). An already-configured mechanism for this user →
// *kafka.Error{Kind: AlreadyExists, Resource: "user"}.
func (s *Service) Create(ctx context.Context, caller core.Caller, params CreateParams) (CreateResult, error) {
	if err := s.checkGate(ctx, caller, "S1", params.Name); err != nil {
		return CreateResult{}, err
	}

	iterations := params.Iterations
	if iterations == 0 {
		iterations = DefaultIterations
	}

	attempt := s.newEvent(caller, "S1", params.Name)
	if err := s.auditor.Attempt(ctx, attempt); err != nil {
		return CreateResult{}, &core.PolicyError{Code: core.AuditUnavailable, Message: err.Error()}
	}

	result, err := s.createOne(ctx, params.Name, params.Mechanism, params.Password, iterations)
	if err != nil {
		s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeFailed, errCodeOf(err)))
		return CreateResult{}, err
	}
	s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeSucceeded, ""))
	return result, nil
}

// createOne checks name/mechanism isn't already configured, then upserts it.
// AlterUserSCRAMs's own upsert is silently idempotent; S1 create must
// instead reject an existing credential, matching T5's own existence check
// ahead of a broker call that would otherwise happily overwrite it.
func (s *Service) createOne(ctx context.Context, name string, mechanism kafka.ScramMechanism, password string, iterations int32) (CreateResult, error) {
	exists, err := s.userHasMechanism(ctx, name, mechanism)
	if err != nil {
		return CreateResult{}, err
	}
	if exists {
		return CreateResult{}, &kafka.Error{Kind: kafka.KindAlreadyExists, Resource: "user"}
	}

	results, err := s.admin.AlterUserSCRAMs(ctx, []kafka.ScramUpsert{
		{User: name, Mechanism: mechanism, Iterations: iterations, Password: password},
	}, nil)
	if err != nil {
		return CreateResult{}, err
	}
	if results[0].Err != nil {
		return CreateResult{}, results[0].Err
	}
	return CreateResult{Name: name, Mechanism: mechanism}, nil
}

// userHasMechanism reports whether name already has a credential configured
// for mechanism. A user that does not exist at all reports false, not
// NotFound — DescribeUserSCRAMs' NotFound means "no such user," which is
// exactly the case createOne wants to treat as "safe to create."
func (s *Service) userHasMechanism(ctx context.Context, name string, mechanism kafka.ScramMechanism) (bool, error) {
	users, err := s.admin.DescribeUserSCRAMs(ctx, name)
	if err != nil {
		if kafka.IsKind(err, kafka.KindNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, c := range users[0].Credentials {
		if c.Mechanism == mechanism {
			return true, nil
		}
	}
	return false, nil
}

// QuotaItem is one entity's configured quotas in a ListQuotas result
// (FUNC-SPEC §8.7 S2 list).
type QuotaItem struct {
	Entity kafka.QuotaEntity
	Values []kafka.QuotaValue
}

// ListQuotas returns every configured client quota, optionally restricted
// to one entity type, sorted by entity descriptor for a stable response
// (FUNC-SPEC §8.7 S2 list).
func (s *Service) ListQuotas(ctx context.Context, entityType string) ([]QuotaItem, error) {
	described, err := s.admin.DescribeClientQuotas(ctx, entityType)
	if err != nil {
		return nil, err
	}
	sort.Slice(described, func(i, j int) bool {
		return entityDescriptor(described[i].Entity) < entityDescriptor(described[j].Entity)
	})
	out := make([]QuotaItem, len(described))
	for i, d := range described {
		out[i] = QuotaItem{Entity: d.Entity, Values: d.Values}
	}
	return out, nil
}
