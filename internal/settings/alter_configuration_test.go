package settings

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeDescribeConfigurations struct {
	entries  []Configuration
	err      error
	resource Resource
	hasLimit bool
}

func (f *fakeDescribeConfigurations) Execute(ctx context.Context, resource Resource) ([]Configuration, error) {
	f.resource = resource
	_, f.hasLimit = ctx.Deadline()
	return f.entries, f.err
}

type fakeAlterKafkaConfiguration struct {
	calls       []bool
	resource    Resource
	change      Change
	validateErr error
	applyErr    error
}

func (f *fakeAlterKafkaConfiguration) Execute(_ context.Context, resource Resource, change Change, validateOnly bool) error {
	f.calls = append(f.calls, validateOnly)
	f.resource = resource
	f.change = change
	if validateOnly {
		return f.validateErr
	}
	return f.applyErr
}

func TestAlterConfigurationValidatesBeforeApplying(t *testing.T) {
	value := "604800000"
	resource := Resource{Kind: Topic, Name: "orders"}
	change := Change{Name: "retention.ms", Operation: "set", Value: &value}
	describe := &fakeDescribeConfigurations{entries: []Configuration{{Name: change.Name}}}
	alter := &fakeAlterKafkaConfiguration{}

	err := NewAlterConfiguration(describe, alter, time.Second).Execute(context.Background(), resource, change)
	if err != nil {
		t.Fatal(err)
	}
	if len(alter.calls) != 2 || !alter.calls[0] || alter.calls[1] {
		t.Fatalf("calls = %v, want validation followed by a single apply", alter.calls)
	}
	if describe.resource != resource || alter.resource != resource || alter.change != change || !describe.hasLimit {
		t.Fatalf("resource, change or deadline was not passed through: %#v %#v", describe, alter)
	}
}

func TestAlterConfigurationDoesNotWriteUnsafeOrInvalidSettings(t *testing.T) {
	value := "false"
	for _, test := range []struct {
		name     string
		resource Resource
		change   Change
		entries  []Configuration
		want     error
	}{
		{"invalid topic", Resource{Kind: Topic, Name: ".."}, Change{Name: "x", Operation: "set", Value: &value}, nil, ErrInvalidResource},
		{"invalid broker", Resource{Kind: Broker, Name: "-1"}, Change{Name: "x", Operation: "set", Value: &value}, nil, ErrInvalidResource},
		{"missing value", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "set"}, nil, ErrInvalidChange},
		{"reset with value", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "reset", Value: &value}, nil, ErrInvalidChange},
		{"oversized value", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "set", Value: stringPointer(strings.Repeat("x", 32769))}, nil, ErrInvalidChange},
		{"missing key", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "reset"}, nil, ErrNotFound},
		{"static key", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "reset"}, []Configuration{{Name: "x", ReadOnly: true}}, ErrNotEditable},
		{"sensitive key", Resource{Kind: Broker, Name: "0"}, Change{Name: "x", Operation: "reset"}, []Configuration{{Name: "x", Sensitive: true}}, ErrNotEditable},
	} {
		t.Run(test.name, func(t *testing.T) {
			describe := &fakeDescribeConfigurations{entries: test.entries}
			alter := &fakeAlterKafkaConfiguration{}
			err := NewAlterConfiguration(describe, alter, time.Second).Execute(context.Background(), test.resource, test.change)
			if !errors.Is(err, test.want) || len(alter.calls) != 0 {
				t.Fatalf("error = %v, calls = %v, want %v and no writes", err, alter.calls, test.want)
			}
		})
	}
}

func TestAlterConfigurationReportsValidationAndUnknownOutcomes(t *testing.T) {
	value := "compact"
	change := Change{Name: "cleanup.policy", Operation: "set", Value: &value}
	resource := Resource{Kind: Topic, Name: "orders"}
	describe := &fakeDescribeConfigurations{entries: []Configuration{{Name: change.Name}}}

	alter := &fakeAlterKafkaConfiguration{validateErr: ErrRejected}
	err := NewAlterConfiguration(describe, alter, time.Second).Execute(context.Background(), resource, change)
	if !errors.Is(err, ErrRejected) || len(alter.calls) != 1 || !alter.calls[0] {
		t.Fatalf("validation error = %v, calls = %v", err, alter.calls)
	}

	alter = &fakeAlterKafkaConfiguration{applyErr: context.DeadlineExceeded}
	err = NewAlterConfiguration(describe, alter, time.Second).Execute(context.Background(), resource, change)
	if !errors.Is(err, ErrWriteUnknown) || len(alter.calls) != 2 {
		t.Fatalf("apply error = %v, calls = %v", err, alter.calls)
	}
}

func stringPointer(value string) *string { return &value }
