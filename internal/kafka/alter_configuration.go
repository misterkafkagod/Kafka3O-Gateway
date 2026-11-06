package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

type configurationAdmin interface {
	ValidateAlterTopicConfigs(context.Context, []kadm.AlterConfig, ...string) (kadm.AlterConfigsResponses, error)
	AlterTopicConfigs(context.Context, []kadm.AlterConfig, ...string) (kadm.AlterConfigsResponses, error)
	ValidateAlterBrokerConfigs(context.Context, []kadm.AlterConfig, ...int32) (kadm.AlterConfigsResponses, error)
	AlterBrokerConfigs(context.Context, []kadm.AlterConfig, ...int32) (kadm.AlterConfigsResponses, error)
}

type alterConfiguration struct {
	admin configurationAdmin
}

func NewAlterConfiguration(admin configurationAdmin) settings.AlterKafkaConfiguration {
	return alterConfiguration{admin: admin}
}

func (a alterConfiguration) Execute(ctx context.Context, resource settings.Resource, change settings.Change, validateOnly bool) error {
	config := kadm.AlterConfig{Name: change.Name, Value: change.Value}
	switch change.Operation {
	case "set":
		config.Op = kadm.SetConfig
	case "reset":
		config.Op = kadm.DeleteConfig
	default:
		return settings.ErrInvalidChange
	}
	var responses kadm.AlterConfigsResponses
	var err error
	switch resource.Kind {
	case settings.Topic:
		if validateOnly {
			responses, err = a.admin.ValidateAlterTopicConfigs(ctx, []kadm.AlterConfig{config}, resource.Name)
		} else {
			responses, err = a.admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{config}, resource.Name)
		}
	case settings.Broker:
		id, parseErr := strconv.ParseInt(resource.Name, 10, 32)
		if parseErr != nil || id < 0 {
			return settings.ErrInvalidResource
		}
		if validateOnly {
			responses, err = a.admin.ValidateAlterBrokerConfigs(ctx, []kadm.AlterConfig{config}, int32(id))
		} else {
			responses, err = a.admin.AlterBrokerConfigs(ctx, []kadm.AlterConfig{config}, int32(id))
		}
	default:
		return settings.ErrInvalidResource
	}
	if err != nil {
		return fmt.Errorf("alter configuration: %w", configurationError(err))
	}
	response, err := responses.On(resource.Name, nil)
	if err != nil {
		return fmt.Errorf("alter configuration response: %w", configurationError(err))
	}
	if response.Err != nil {
		return configurationError(response.Err)
	}
	return nil
}
