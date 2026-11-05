package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeTopicConfigDescriber struct {
	configs kadm.ResourceConfigs
	err     error
	calls   int
	ctx     context.Context
	names   []string
}

func (f *fakeTopicConfigDescriber) DescribeTopicConfigs(ctx context.Context, names ...string) (kadm.ResourceConfigs, error) {
	f.calls++
	f.ctx = ctx
	f.names = names
	return f.configs, f.err
}

func TestTopicOffsetError_ConfirmsMissingTopic(t *testing.T) {
	shardErr := &kadm.ShardErrors{Name: "ListOffsets", Errs: []kadm.ShardError{{Err: kerr.UnknownTopicOrPartition}}}
	tests := []struct {
		name    string
		cause   error
		configs kadm.ResourceConfigs
		err     error
	}{
		{name: "missing config", cause: shardErr, configs: kadm.ResourceConfigs{{Name: "orders", Err: kerr.UnknownTopicOrPartition}}},
		{name: "missing topic ID", cause: kerr.UnknownTopicID, configs: kadm.ResourceConfigs{{Name: "orders", Err: kerr.UnknownTopicID}}},
		{name: "missing request", cause: kerr.UnknownTopicOrPartition, err: kerr.UnknownTopicOrPartition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admin := &fakeTopicConfigDescriber{configs: tt.configs, err: tt.err}
			ctx := context.Background()
			got := topicOffsetError(ctx, admin, "orders", tt.cause)
			if !errors.Is(got, topics.ErrTopicNotFound) {
				t.Fatalf("error = %v, want topic not found", got)
			}
			if admin.calls != 1 || admin.ctx != ctx || len(admin.names) != 1 || admin.names[0] != "orders" {
				t.Fatalf("config check = %#v", admin)
			}
		})
	}
}

func TestTopicOffsetError_PreservesOtherFailures(t *testing.T) {
	offline := errors.New("broker unavailable")
	admin := &fakeTopicConfigDescriber{}
	if got := topicOffsetError(context.Background(), admin, "orders", offline); !errors.Is(got, offline) || admin.calls != 0 {
		t.Fatalf("unrelated error = %v, calls = %d", got, admin.calls)
	}

	admin.configs = kadm.ResourceConfigs{{Name: "orders"}}
	unknownPartition := fmt.Errorf("offset request: %w", kerr.UnknownTopicOrPartition)
	if got := topicOffsetError(context.Background(), admin, "orders", unknownPartition); !errors.Is(got, unknownPartition) ||
		errors.Is(got, topics.ErrTopicNotFound) {
		t.Fatalf("existing topic error = %v", got)
	}

	admin.configs = nil
	if got := topicOffsetError(context.Background(), admin, "orders", unknownPartition); !errors.Is(got, unknownPartition) ||
		errors.Is(got, topics.ErrTopicNotFound) {
		t.Fatalf("missing config result error = %v", got)
	}

	admin.err = kerr.TopicAuthorizationFailed
	got := topicOffsetError(context.Background(), admin, "orders", unknownPartition)
	if !errors.Is(got, unknownPartition) || !errors.Is(got, kerr.TopicAuthorizationFailed) ||
		errors.Is(got, topics.ErrTopicNotFound) {
		t.Fatalf("failed config check error = %v", got)
	}
	admin.err = nil
	admin.configs = kadm.ResourceConfigs{{Name: "orders", Err: kerr.TopicAuthorizationFailed}}
	got = topicOffsetError(context.Background(), admin, "orders", unknownPartition)
	if !errors.Is(got, unknownPartition) || !errors.Is(got, kerr.TopicAuthorizationFailed) ||
		errors.Is(got, topics.ErrTopicNotFound) {
		t.Fatalf("failed config result error = %v", got)
	}
}
