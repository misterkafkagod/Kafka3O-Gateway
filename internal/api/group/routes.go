package group

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires G1 and G2 onto humaAPI (FUNC-SPEC §8.7 G1, G2; TECH-SPEC
// §6.2). G3 is registered by internal/api/topic instead (see package doc).
// pageBounds is the configured ?pageSize= default and ceiling (FUNC-SPEC §8.8).
func Register(humaAPI huma.API, svc *group.Service, pageBounds PageBounds) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-list",
		Method:      http.MethodGet,
		Path:        "/v1/consumer-groups",
		Summary:     "List consumer groups",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G1"),
	}, listGroups(svc, pageBounds))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-describe",
		Method:      http.MethodGet,
		Path:        "/v1/consumer-groups/{groupId}",
		Summary:     "Describe a consumer group",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G2"),
	}, describeGroup(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-reset-offsets",
		Method:      http.MethodPost,
		Path:        "/v1/consumer-groups/{groupId}/reset-offsets",
		Summary:     "Reset a consumer group's committed offsets",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G4"),
	}, resetOffsets(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-delete",
		Method:      http.MethodDelete,
		Path:        "/v1/consumer-groups/{groupId}",
		Summary:     "Delete a consumer group",
		Description: "Carries a JSON body ({confirm}) on a DELETE request; some intermediaries strip DELETE bodies, in which case Confirm arrives empty and the request fails closed as 400 CONFIRMATION_MISMATCH (TECH-SPEC R3).",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G5"),
	}, deleteGroup(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-remove-members",
		Method:      http.MethodPost,
		Path:        "/v1/consumer-groups/{groupId}/remove-members",
		Summary:     "Evict members from a consumer group",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G6"),
	}, removeMembers(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "group-clone-offsets",
		Method:      http.MethodPost,
		Path:        "/v1/consumer-groups/{target}/clone-offsets",
		Summary:     "Clone committed offsets from another consumer group",
		Tags:        []string{"Consumer Groups"},
		Extensions:  commandExtension("G7"),
	}, cloneOffsets(svc))
}

func resetOffsets(svc *group.Service) func(context.Context, *ResetOffsetsInput) (*ResetOffsetsOutput, error) {
	return func(ctx context.Context, in *ResetOffsetsInput) (*ResetOffsetsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		target, err := toResetTarget(in.Body.Target)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}

		result, err := svc.Reset(ctx, caller, in.GroupID, target, in.Body.Topics, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p group.ResetPlan) any { return toResetOffsetsPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &ResetOffsetsOutput{Body: ResetOffsetsBody{DryRun: true, Plan: toResetOffsetsPlanDTO(result.Plan)}}, nil
		}
		return &ResetOffsetsOutput{Body: ResetOffsetsBody{
			GroupID: result.Value.GroupID, Offsets: toResetOffsetResultDTOs(result.Value.Offsets),
		}}, nil
	}
}

func deleteGroup(svc *group.Service) func(context.Context, *DeleteGroupInput) (*DeleteGroupOutput, error) {
	return func(ctx context.Context, in *DeleteGroupInput) (*DeleteGroupOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Delete(ctx, caller, in.GroupID, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p group.DeleteGroupPlan) any { return toDeleteGroupPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &DeleteGroupOutput{Body: DeleteGroupBody{DryRun: true, Plan: toDeleteGroupPlanDTO(result.Plan)}}, nil
		}
		return &DeleteGroupOutput{Body: DeleteGroupBody{Deleted: result.Value.Deleted}}, nil
	}
}

func removeMembers(svc *group.Service) func(context.Context, *RemoveMembersInput) (*RemoveMembersOutput, error) {
	return func(ctx context.Context, in *RemoveMembersInput) (*RemoveMembersOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.RemoveMembers(ctx, caller, in.GroupID, in.Body.Members, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p group.RemoveMembersPlan) any { return toRemoveMembersPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &RemoveMembersOutput{Body: RemoveMembersBody{DryRun: true, Plan: toRemoveMembersPlanDTO(result.Plan)}}, nil
		}
		return &RemoveMembersOutput{Body: RemoveMembersBody{Removed: result.Value.Removed}}, nil
	}
}

func cloneOffsets(svc *group.Service) func(context.Context, *CloneOffsetsInput) (*CloneOffsetsOutput, error) {
	return func(ctx context.Context, in *CloneOffsetsInput) (*CloneOffsetsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.CloneOffsets(ctx, caller, in.Target, in.Body.Source, in.Body.Topics, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p group.CloneOffsetsPlan) any { return toCloneOffsetsPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &CloneOffsetsOutput{Body: CloneOffsetsBody{DryRun: true, Plan: toCloneOffsetsPlanDTO(result.Plan)}}, nil
		}
		return &CloneOffsetsOutput{Body: CloneOffsetsBody{
			Target: result.Value.Target, Offsets: toCloneOffsetResultDTOs(result.Value.Offsets),
		}}, nil
	}
}

func listGroups(svc *group.Service, bounds PageBounds) func(context.Context, *ListGroupsInput) (*ListGroupsOutput, error) {
	return func(ctx context.Context, in *ListGroupsInput) (*ListGroupsOutput, error) {
		pageSize := in.PageSize
		if pageSize == 0 {
			pageSize = bounds.Default
		}
		if pageSize > bounds.Ceiling {
			return nil, apierrors.Map(&core.PolicyError{
				Code:    core.BoundExceeded,
				Message: "pageSize exceeds the configured ceiling",
			}, apierrors.RequestIDFrom(ctx))
		}

		items, page, err := svc.List(ctx, group.ListParams{State: in.State, Page: in.Page, PageSize: pageSize})
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ListGroupsOutput{Body: ListGroupsBody{
			Items: toGroupSummaryDTOs(items),
			Page:  GroupPageDTO{Page: page.Page, PageSize: page.PageSize, Total: page.Total},
		}}, nil
	}
}

func describeGroup(svc *group.Service) func(context.Context, *DescribeGroupInput) (*DescribeGroupOutput, error) {
	return func(ctx context.Context, in *DescribeGroupInput) (*DescribeGroupOutput, error) {
		d, err := svc.Describe(ctx, in.GroupID)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeGroupOutput{Body: toDescribeGroupBody(d)}, nil
	}
}
