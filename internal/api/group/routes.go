package group

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
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
