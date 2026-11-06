package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

type configurationRequester interface {
	RequestSharded(context.Context, kmsg.Request) []kgo.ResponseShard
}

type describeConfigurations struct {
	requester configurationRequester
}

func NewDescribeConfigurations(requester configurationRequester) settings.DescribeConfigurations {
	return describeConfigurations{requester: requester}
}

func (d describeConfigurations) Execute(ctx context.Context, resource settings.Resource) ([]settings.Configuration, error) {
	request := kmsg.NewPtrDescribeConfigsRequest()
	request.IncludeSynonyms = true
	request.IncludeDocumentation = true
	item := kmsg.NewDescribeConfigsRequestResource()
	item.ResourceName = resource.Name
	switch resource.Kind {
	case settings.Topic:
		item.ResourceType = kmsg.ConfigResourceTypeTopic
	case settings.Broker:
		item.ResourceType = kmsg.ConfigResourceTypeBroker
	default:
		return nil, settings.ErrInvalidResource
	}
	request.Resources = append(request.Resources, item)
	shards := d.requester.RequestSharded(ctx, request)
	result := make([]settings.Configuration, 0)
	found := false
	for _, shard := range shards {
		if shard.Err != nil {
			return nil, fmt.Errorf("describe configuration: %w", configurationError(shard.Err))
		}
		response, ok := shard.Resp.(*kmsg.DescribeConfigsResponse)
		if !ok || response == nil {
			return nil, errors.New("describe configuration: invalid Kafka response")
		}
		for _, described := range response.Resources {
			if described.ResourceType != item.ResourceType || described.ResourceName != item.ResourceName || found {
				return nil, errors.New("describe configuration: unexpected Kafka resource")
			}
			found = true
			if err := kerr.ErrorForCode(described.ErrorCode); err != nil {
				return nil, configurationError(err)
			}
			for _, entry := range described.Configs {
				sensitive := entry.IsSensitive || entry.ConfigType == kmsg.ConfigTypePassword
				dynamicSource := entry.Source == kmsg.ConfigSourceDynamicTopicConfig ||
					entry.Source == kmsg.ConfigSourceDynamicBrokerConfig ||
					entry.Source == kmsg.ConfigSourceDynamicDefaultBrokerConfig
				mapped := settings.Configuration{
					Name:      entry.Name,
					Sensitive: sensitive,
					ReadOnly:  entry.ReadOnly && !dynamicSource,
					Source:    entry.Source.String(),
					Type:      entry.ConfigType.String(),
				}
				if !sensitive {
					mapped.Value = entry.Value
					if entry.Documentation != nil {
						mapped.Documentation = *entry.Documentation
					}
				}
				result = append(result, mapped)
			}
		}
	}
	if !found {
		return nil, errors.New("describe configuration: Kafka omitted the requested resource")
	}
	slices.SortFunc(result, func(a, b settings.Configuration) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result, nil
}
