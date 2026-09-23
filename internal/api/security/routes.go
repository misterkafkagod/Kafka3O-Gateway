package security

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/security"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires the security commands onto humaAPI (FUNC-SPEC §8.7 S1, S2;
// TECH-SPEC §6.2).
func Register(humaAPI huma.API, svc *security.Service) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "security-users-list",
		Method:      http.MethodGet,
		Path:        "/v1/scram-users",
		Summary:     "List SCRAM users",
		Tags:        []string{"Security"},
		Extensions:  commandExtension("S1"),
	}, listUsers(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "security-users-create",
		Method:      http.MethodPost,
		Path:        "/v1/scram-users",
		Summary:     "Create a SCRAM user credential",
		Tags:        []string{"Security"},
		Extensions:  commandExtension("S1"),
	}, createUser(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "security-users-delete",
		Method:      http.MethodDelete,
		Path:        "/v1/scram-users/{name}",
		Summary:     "Delete a SCRAM user credential",
		Description: "Carries a JSON body ({confirm, mechanism?}) on a DELETE request; some intermediaries strip DELETE bodies, in which case Confirm arrives empty and the request fails closed as 400 CONFIRMATION_MISMATCH (TECH-SPEC R3).",
		Tags:        []string{"Security"},
		Extensions:  commandExtension("S1"),
	}, deleteUser(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "security-quotas-list",
		Method:      http.MethodGet,
		Path:        "/v1/quotas",
		Summary:     "List client quotas",
		Tags:        []string{"Security"},
		Extensions:  commandExtension("S2"),
	}, listQuotas(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "security-quotas-alter",
		Method:      http.MethodPatch,
		Path:        "/v1/quotas",
		Summary:     "Set or remove a client quota's values",
		Tags:        []string{"Security"},
		Extensions:  commandExtension("S2"),
	}, alterQuota(svc))
}

func listUsers(svc *security.Service) func(context.Context, *ListUsersInput) (*ListUsersOutput, error) {
	return func(ctx context.Context, in *ListUsersInput) (*ListUsersOutput, error) {
		users, err := svc.ListUsers(ctx)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ListUsersOutput{Body: ListUsersBody{Users: toScramUserDTOs(users)}}, nil
	}
}

func createUser(svc *security.Service) func(context.Context, *CreateUserInput) (*CreateUserOutput, error) {
	return func(ctx context.Context, in *CreateUserInput) (*CreateUserOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Create(ctx, caller, security.CreateParams{
			Name:       in.Body.Name,
			Mechanism:  kafka.ScramMechanism(in.Body.Mechanism),
			Password:   in.Body.Password,
			Iterations: in.Body.Iterations,
		})
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &CreateUserOutput{Status: http.StatusCreated, Body: CreateUserBody{
			Name: result.Name, Mechanism: string(result.Mechanism),
		}}, nil
	}
}

func deleteUser(svc *security.Service) func(context.Context, *DeleteUserInput) (*DeleteUserOutput, error) {
	return func(ctx context.Context, in *DeleteUserInput) (*DeleteUserOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Delete(ctx, caller, in.Name, kafka.ScramMechanism(in.Body.Mechanism), in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p security.DeletePlan) any { return toDeleteUserPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &DeleteUserOutput{Body: DeleteUserBody{DryRun: true, Plan: toDeleteUserPlanDTO(result.Plan)}}, nil
		}
		return &DeleteUserOutput{Body: DeleteUserBody{Deleted: result.Value.Deleted}}, nil
	}
}

func listQuotas(svc *security.Service) func(context.Context, *ListQuotasInput) (*ListQuotasOutput, error) {
	return func(ctx context.Context, in *ListQuotasInput) (*ListQuotasOutput, error) {
		items, err := svc.ListQuotas(ctx, in.EntityType)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ListQuotasOutput{Body: ListQuotasBody{Items: toQuotaItemDTOs(items)}}, nil
	}
}

func alterQuota(svc *security.Service) func(context.Context, *AlterQuotaInput) (*AlterQuotaOutput, error) {
	return func(ctx context.Context, in *AlterQuotaInput) (*AlterQuotaOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		entity := toQuotaEntity(in.Body.Entity)
		result, err := svc.AlterQuota(ctx, caller, entity, toQuotaSet(in.Body.Set), toQuotaRemove(in.Body.Remove), in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p security.AlterQuotaPlan) any { return toAlterQuotaPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &AlterQuotaOutput{Body: AlterQuotaBody{DryRun: true, Plan: toAlterQuotaPlanDTO(result.Plan)}}, nil
		}
		entityDTO := toQuotaEntityDTO(result.Value.Entity)
		quotasDTO := toQuotaValuesDTO(result.Value.Values)
		return &AlterQuotaOutput{Body: AlterQuotaBody{Entity: &entityDTO, Quotas: &quotasDTO}}, nil
	}
}
