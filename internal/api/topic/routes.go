package topic

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
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
