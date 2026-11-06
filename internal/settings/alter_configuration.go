package settings

import (
	"context"
	"fmt"
	"time"
)

type AlterConfiguration interface {
	Execute(ctx context.Context, resource Resource, change Change) error
}

type alterConfiguration struct {
	describe DescribeConfigurations
	alter    AlterKafkaConfiguration
	timeout  time.Duration
}

func NewAlterConfiguration(describe DescribeConfigurations, alter AlterKafkaConfiguration, timeout time.Duration) AlterConfiguration {
	return alterConfiguration{describe: describe, alter: alter, timeout: timeout}
}

func (a alterConfiguration) Execute(ctx context.Context, resource Resource, change Change) error {
	if !IsValidResource(resource) {
		return ErrInvalidResource
	}
	if !IsValidChange(change) {
		return ErrInvalidChange
	}
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	configurations, err := a.describe.Execute(ctx, resource)
	if err != nil {
		return err
	}
	found := false
	for _, entry := range configurations {
		if entry.Name == change.Name {
			found = true
			if entry.Sensitive || entry.ReadOnly {
				return ErrNotEditable
			}
			break
		}
	}
	if !found {
		return ErrNotFound
	}
	if err := a.alter.Execute(ctx, resource, change, true); err != nil {
		return err
	}
	if err := a.alter.Execute(ctx, resource, change, false); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteUnknown, err)
	}
	return nil
}
