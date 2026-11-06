package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

type fakeConfigurationRequester struct {
	request *kmsg.DescribeConfigsRequest
	shards  []kgo.ResponseShard
}

func (f *fakeConfigurationRequester) RequestSharded(_ context.Context, request kmsg.Request) []kgo.ResponseShard {
	f.request = request.(*kmsg.DescribeConfigsRequest)
	return f.shards
}

func TestDescribeConfigurationsMapsMetadataAndHidesSensitiveValues(t *testing.T) {
	retention, secret, doc := "60000", "not-for-output", "Retention period"
	requester := &fakeConfigurationRequester{shards: []kgo.ResponseShard{{Resp: &kmsg.DescribeConfigsResponse{
		Resources: []kmsg.DescribeConfigsResponseResource{{
			ResourceType: kmsg.ConfigResourceTypeBroker,
			ResourceName: "1",
			Configs: []kmsg.DescribeConfigsResponseResourceConfig{
				{Name: "password", Value: &secret, IsSensitive: true, Documentation: &secret},
				{Name: "password-type", Value: &secret, ConfigType: kmsg.ConfigTypePassword},
				{Name: "auto.create.topics.enable", ReadOnly: true, Source: kmsg.ConfigSourceStaticBrokerConfig},
				{Name: "retention.ms", Value: &retention, ReadOnly: true, Source: kmsg.ConfigSourceDynamicBrokerConfig, Documentation: &doc},
			},
		}},
	}}}}
	got, err := NewDescribeConfigurations(requester).Execute(context.Background(), settings.Resource{Kind: settings.Broker, Name: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0].Name != "auto.create.topics.enable" || !got[0].ReadOnly ||
		got[1].Name != "password" || !got[1].Sensitive || got[1].Value != nil || got[1].Documentation != "" ||
		got[2].Name != "password-type" || !got[2].Sensitive || got[2].Value != nil ||
		got[3].Name != "retention.ms" || got[3].ReadOnly || *got[3].Value != retention || got[3].Documentation != doc {
		t.Fatalf("unexpected mapped configurations: %#v", got)
	}
	if !requester.request.IncludeSynonyms || !requester.request.IncludeDocumentation ||
		len(requester.request.Resources) != 1 || requester.request.Resources[0].ResourceName != "1" {
		t.Fatalf("unexpected Kafka request: %#v", requester.request)
	}
}

func TestDescribeConfigurationsRejectsMissingWrongAndDeniedResources(t *testing.T) {
	resource := settings.Resource{Kind: settings.Topic, Name: "orders"}
	for _, test := range []struct {
		name   string
		shards []kgo.ResponseShard
		want   error
	}{
		{"no response", nil, nil},
		{"wrong resource", []kgo.ResponseShard{{Resp: &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "other"}}}}}, nil},
		{"denied", []kgo.ResponseShard{{Resp: &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "orders", ErrorCode: kerr.TopicAuthorizationFailed.Code}}}}}, settings.ErrDenied},
		{"missing topic", []kgo.ResponseShard{{Resp: &kmsg.DescribeConfigsResponse{Resources: []kmsg.DescribeConfigsResponseResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "orders", ErrorCode: kerr.UnknownTopicOrPartition.Code}}}}}, settings.ErrNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewDescribeConfigurations(&fakeConfigurationRequester{shards: test.shards}).Execute(context.Background(), resource)
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}
