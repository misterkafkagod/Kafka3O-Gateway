package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

type fakeConfigurationAdmin struct {
	called    string
	configs   []kadm.AlterConfig
	topics    []string
	brokers   []int32
	responses kadm.AlterConfigsResponses
	err       error
}

func (f *fakeConfigurationAdmin) ValidateAlterTopicConfigs(_ context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	f.called, f.configs, f.topics = "validate-topic", configs, topics
	return f.responses, f.err
}
func (f *fakeConfigurationAdmin) AlterTopicConfigs(_ context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	f.called, f.configs, f.topics = "alter-topic", configs, topics
	return f.responses, f.err
}
func (f *fakeConfigurationAdmin) ValidateAlterBrokerConfigs(_ context.Context, configs []kadm.AlterConfig, brokers ...int32) (kadm.AlterConfigsResponses, error) {
	f.called, f.configs, f.brokers = "validate-broker", configs, brokers
	return f.responses, f.err
}
func (f *fakeConfigurationAdmin) AlterBrokerConfigs(_ context.Context, configs []kadm.AlterConfig, brokers ...int32) (kadm.AlterConfigsResponses, error) {
	f.called, f.configs, f.brokers = "alter-broker", configs, brokers
	return f.responses, f.err
}

func TestAlterConfigurationAdapterUsesIncrementalSetAndReset(t *testing.T) {
	value := "compact"
	for _, test := range []struct {
		resource settings.Resource
		change   settings.Change
		validate bool
		called   string
		op       kadm.IncrementalOp
	}{
		{settings.Resource{Kind: settings.Topic, Name: "orders"}, settings.Change{Name: "cleanup.policy", Operation: "set", Value: &value}, true, "validate-topic", kadm.SetConfig},
		{settings.Resource{Kind: settings.Topic, Name: "orders"}, settings.Change{Name: "cleanup.policy", Operation: "reset"}, false, "alter-topic", kadm.DeleteConfig},
		{settings.Resource{Kind: settings.Broker, Name: "3"}, settings.Change{Name: "log.retention.ms", Operation: "set", Value: &value}, true, "validate-broker", kadm.SetConfig},
		{settings.Resource{Kind: settings.Broker, Name: "3"}, settings.Change{Name: "log.retention.ms", Operation: "reset"}, false, "alter-broker", kadm.DeleteConfig},
	} {
		admin := &fakeConfigurationAdmin{responses: kadm.AlterConfigsResponses{{Name: test.resource.Name}}}
		err := NewAlterConfiguration(admin).Execute(context.Background(), test.resource, test.change, test.validate)
		if err != nil || admin.called != test.called || len(admin.configs) != 1 ||
			admin.configs[0].Op != test.op || admin.configs[0].Name != test.change.Name || admin.configs[0].Value != test.change.Value {
			t.Fatalf("error %v, admin %#v", err, admin)
		}
		if test.resource.Kind == settings.Broker && (len(admin.brokers) != 1 || admin.brokers[0] != 3) {
			t.Fatalf("wrong broker target: %v", admin.brokers)
		}
	}
}

func TestAlterConfigurationAdapterMapsKafkaRejectionAndAuthorization(t *testing.T) {
	resource := settings.Resource{Kind: settings.Topic, Name: "orders"}
	change := settings.Change{Name: "cleanup.policy", Operation: "reset"}
	for _, test := range []struct {
		kafkaErr error
		want     error
	}{
		{kerr.InvalidConfig, settings.ErrRejected},
		{kerr.TopicAuthorizationFailed, settings.ErrDenied},
		{kerr.UnknownTopicOrPartition, settings.ErrNotFound},
	} {
		admin := &fakeConfigurationAdmin{responses: kadm.AlterConfigsResponses{{Name: "orders", Err: test.kafkaErr}}}
		err := NewAlterConfiguration(admin).Execute(context.Background(), resource, change, true)
		if !errors.Is(err, test.want) {
			t.Errorf("Kafka error %v: got %v, want %v", test.kafkaErr, err, test.want)
		}
	}
}
