package topic

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

// commandIDExtension is the x-command-id key every operation carries
// (TECH-SPEC O5).
const commandIDExtension = "x-command-id"

func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// Register wires the topic commands onto humaAPI, plus G3 (FUNC-SPEC §8.7
// T1-T4, G3; TECH-SPEC §6.2: G3 lives on the topics path even though its
// service method, groupSvc.ConsumersOfTopic, belongs to the group service).
// pageBounds is the configured ?pageSize= default and ceiling (FUNC-SPEC §8.8).
func Register(humaAPI huma.API, svc *topic.Service, groupSvc *group.Service, pageBounds PageBounds) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-list",
		Method:      http.MethodGet,
		Path:        "/v1/topics",
		Summary:     "List topics",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T1"),
	}, listTopics(svc, pageBounds))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-describe",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}",
		Summary:     "Describe a topic",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T2"),
	}, describeTopic(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-size",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}/size",
		Summary:     "Topic on-disk size",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T3"),
	}, topicSize(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-count",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}/count",
		Summary:     "Message count in a time window",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T4"),
	}, topicCount(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-consumer-groups",
		Method:      http.MethodGet,
		Path:        "/v1/topics/{name}/consumer-groups",
		Summary:     "Consumer groups for a topic",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("G3"),
	}, topicConsumerGroups(groupSvc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-create",
		Method:      http.MethodPost,
		Path:        "/v1/topics",
		Summary:     "Create a topic",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T5"),
	}, createTopic(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-create-bulk",
		Method:      http.MethodPost,
		Path:        "/v1/batch/topics",
		Summary:     "Bulk create topics",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T6"),
	}, createTopicsBulk(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-alter-config",
		Method:      http.MethodPatch,
		Path:        "/v1/topics/{name}/config",
		Summary:     "Alter topic configuration",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T9"),
	}, alterConfig(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-add-partitions",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/partitions",
		Summary:     "Add partitions to a topic",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T10"),
	}, addPartitions(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-delete",
		Method:      http.MethodDelete,
		Path:        "/v1/topics/{name}",
		Summary:     "Delete a topic",
		Description: "Carries a JSON body ({confirm}) on a DELETE request; some intermediaries strip DELETE bodies, in which case Confirm arrives empty and the request fails closed as 400 CONFIRMATION_MISMATCH (TECH-SPEC R3).",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T7"),
	}, deleteTopic(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-delete-bulk",
		Method:      http.MethodPost,
		Path:        "/v1/batch/topics/delete",
		Summary:     "Bulk delete topics by list or pattern",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T8"),
	}, deleteTopicsBulk(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-delete-records",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/delete-records",
		Summary:     "Truncate a topic's partitions to given offsets",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T11"),
	}, deleteRecords(svc))

	huma.Register(humaAPI, huma.Operation{
		OperationID: "topic-purge",
		Method:      http.MethodPost,
		Path:        "/v1/topics/{name}/purge",
		Summary:     "Delete every record in a topic",
		Tags:        []string{"Topics"},
		Extensions:  commandExtension("T12"),
	}, purgeTopic(svc))
}

func createTopic(svc *topic.Service) func(context.Context, *CreateTopicInput) (*CreateTopicOutput, error) {
	return func(ctx context.Context, in *CreateTopicInput) (*CreateTopicOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, isDryRun, err := svc.Create(ctx, caller, toCreateParams(in.Body), in.DryRun)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		if isDryRun {
			return &CreateTopicOutput{Status: http.StatusOK, Body: CreateTopicBody{DryRun: true, Plan: toCreateTopicPlanDTO(result)}}, nil
		}
		return &CreateTopicOutput{Status: http.StatusCreated, Body: CreateTopicBody{
			Name: result.Name, Partitions: result.Partitions, ReplicationFactor: result.ReplicationFactor,
			Configs: toTopicConfigEntryDTOs(result.Configs),
		}}, nil
	}
}

func createTopicsBulk(svc *topic.Service) func(context.Context, *CreateTopicsBulkInput) (*CreateTopicsBulkOutput, error) {
	return func(ctx context.Context, in *CreateTopicsBulkInput) (*CreateTopicsBulkOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		params := make([]topic.CreateParams, len(in.Body.Topics))
		for i, t := range in.Body.Topics {
			params[i] = toCreateParams(t)
		}

		result, err := svc.CreateBulk(ctx, caller, params)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		body, status := toCreateTopicsBulkBody(in.Body.Topics, result)
		return &CreateTopicsBulkOutput{Status: status, Body: body}, nil
	}
}

func alterConfig(svc *topic.Service) func(context.Context, *AlterConfigInput) (*AlterConfigOutput, error) {
	return func(ctx context.Context, in *AlterConfigInput) (*AlterConfigOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.AlterConfig(ctx, caller, in.Name, in.Body.Set, in.Body.Reset, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.AlterConfigPlan) any { return toAlterConfigPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &AlterConfigOutput{Body: AlterConfigBody{DryRun: true, Plan: toAlterConfigPlanDTO(result.Plan)}}, nil
		}
		return &AlterConfigOutput{Body: AlterConfigBody{
			Name: result.Value.Name, Configs: toTopicConfigEntryDTOs(result.Value.Configs),
		}}, nil
	}
}

func addPartitions(svc *topic.Service) func(context.Context, *AddPartitionsInput) (*AddPartitionsOutput, error) {
	return func(ctx context.Context, in *AddPartitionsInput) (*AddPartitionsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.AddPartitions(ctx, caller, in.Name, in.Body.Partitions, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.AddPartitionsPlan) any { return toAddPartitionsPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &AddPartitionsOutput{Body: AddPartitionsBody{DryRun: true, Plan: toAddPartitionsPlanDTO(result.Plan)}}, nil
		}
		return &AddPartitionsOutput{Body: AddPartitionsBody{
			Name: result.Value.Name, PartitionCount: result.Value.PartitionCount,
		}}, nil
	}
}

func deleteTopic(svc *topic.Service) func(context.Context, *DeleteTopicInput) (*DeleteTopicOutput, error) {
	return func(ctx context.Context, in *DeleteTopicInput) (*DeleteTopicOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Delete(ctx, caller, in.Name, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.DeleteTopicPlan) any { return toDeleteTopicPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &DeleteTopicOutput{Body: DeleteTopicBody{DryRun: true, Plan: toDeleteTopicPlanDTO(result.Plan)}}, nil
		}
		return &DeleteTopicOutput{Body: DeleteTopicBody{Deleted: result.Value.Deleted}}, nil
	}
}

func deleteTopicsBulk(svc *topic.Service) func(context.Context, *BulkDeleteInput) (*BulkDeleteOutput, error) {
	return func(ctx context.Context, in *BulkDeleteInput) (*BulkDeleteOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.BulkDelete(ctx, caller, in.Body.Topics, in.Body.Pattern, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.BulkDeletePlan) any { return toBulkDeletePlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &BulkDeleteOutput{Status: http.StatusOK, Body: BulkDeleteBody{DryRun: true, Plan: toBulkDeletePlanDTO(result.Plan)}}, nil
		}
		body, status := toBulkDeleteBody(result.Plan.Topics, result.Value)
		return &BulkDeleteOutput{Status: status, Body: body}, nil
	}
}

func deleteRecords(svc *topic.Service) func(context.Context, *DeleteRecordsInput) (*DeleteRecordsOutput, error) {
	return func(ctx context.Context, in *DeleteRecordsInput) (*DeleteRecordsOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		offsets, err := toOffsetsMap(in.Body.Offsets)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		result, err := svc.DeleteRecords(ctx, caller, in.Name, offsets, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.DeleteRecordsPlan) any { return toDeleteRecordsPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &DeleteRecordsOutput{Body: DeleteRecordsBody{DryRun: true, Plan: toDeleteRecordsPlanDTO(result.Plan)}}, nil
		}
		return &DeleteRecordsOutput{Body: DeleteRecordsBody{Partitions: toPartitionWatermarkDTOs(result.Value.Partitions)}}, nil
	}
}

func purgeTopic(svc *topic.Service) func(context.Context, *PurgeInput) (*PurgeOutput, error) {
	return func(ctx context.Context, in *PurgeInput) (*PurgeOutput, error) {
		caller, _ := middleware.CallerFrom(ctx)
		result, err := svc.Purge(ctx, caller, in.Name, in.Body.Confirm, in.DryRun)
		if err != nil {
			return nil, apierrors.MapDestructive(err, func(p topic.DeleteRecordsPlan) any { return toDeleteRecordsPlanDTO(p) }, apierrors.RequestIDFrom(ctx))
		}
		if result.DryRun {
			return &PurgeOutput{Body: PurgeBody{DryRun: true, Plan: toDeleteRecordsPlanDTO(result.Plan)}}, nil
		}
		return &PurgeOutput{Body: PurgeBody{Partitions: toPartitionWatermarkDTOs(result.Value.Partitions)}}, nil
	}
}

func listTopics(svc *topic.Service, bounds PageBounds) func(context.Context, *ListTopicsInput) (*ListTopicsOutput, error) {
	return func(ctx context.Context, in *ListTopicsInput) (*ListTopicsOutput, error) {
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

		items, page, err := svc.List(ctx, topic.ListParams{
			Pattern:         in.Pattern,
			IncludeInternal: in.IncludeInternal,
			Page:            in.Page,
			PageSize:        pageSize,
		})
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &ListTopicsOutput{Body: ListTopicsBody{
			Items: toListItems(items),
			Page:  PageDTO{Page: page.Page, PageSize: page.PageSize, Total: page.Total},
		}}, nil
	}
}

func describeTopic(svc *topic.Service) func(context.Context, *DescribeTopicInput) (*DescribeTopicOutput, error) {
	return func(ctx context.Context, in *DescribeTopicInput) (*DescribeTopicOutput, error) {
		d, err := svc.Describe(ctx, in.Name)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &DescribeTopicOutput{Body: toDescribeBody(d)}, nil
	}
}

func topicSize(svc *topic.Service) func(context.Context, *TopicSizeInput) (*TopicSizeOutput, error) {
	return func(ctx context.Context, in *TopicSizeInput) (*TopicSizeOutput, error) {
		s, err := svc.Size(ctx, in.Name)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &TopicSizeOutput{Body: toSizeBody(s)}, nil
	}
}

func topicCount(svc *topic.Service) func(context.Context, *TopicCountInput) (*TopicCountOutput, error) {
	return func(ctx context.Context, in *TopicCountInput) (*TopicCountOutput, error) {
		requestID := apierrors.RequestIDFrom(ctx)

		from, err := time.Parse(time.RFC3339, in.From)
		if err != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: "from must be an RFC3339 timestamp"}, requestID)
		}
		to, err := time.Parse(time.RFC3339, in.To)
		if err != nil {
			return nil, apierrors.Map(&core.PolicyError{Code: core.Validation, Message: "to must be an RFC3339 timestamp"}, requestID)
		}

		c, err := svc.CountInWindow(ctx, in.Name, from.UnixMilli(), to.UnixMilli())
		if err != nil {
			return nil, apierrors.Map(err, requestID)
		}
		return &TopicCountOutput{Body: toCountBody(c, in.From, in.To)}, nil
	}
}

func topicConsumerGroups(groupSvc *group.Service) func(context.Context, *TopicConsumerGroupsInput) (*TopicConsumerGroupsOutput, error) {
	return func(ctx context.Context, in *TopicConsumerGroupsInput) (*TopicConsumerGroupsOutput, error) {
		groups, err := groupSvc.ConsumersOfTopic(ctx, in.Name)
		if err != nil {
			return nil, apierrors.Map(err, apierrors.RequestIDFrom(ctx))
		}
		return &TopicConsumerGroupsOutput{Body: toTopicConsumerGroupsBody(in.Name, groups)}, nil
	}
}
