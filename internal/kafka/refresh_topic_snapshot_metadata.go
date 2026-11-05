package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func (f *topicSnapshotFetcher) refresh(ctx context.Context) error {
	details, err := f.admin.ListTopicsWithInternal(kadm.WithAuthorizedOps(ctx), f.topic)
	if err != nil {
		return fmt.Errorf("refresh topic metadata: %w", err)
	}
	detail, ok := details[f.topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		return topics.ErrTopicNotFound
	}
	if detail.Err != nil {
		return fmt.Errorf("refresh topic metadata: %w", detail.Err)
	}
	if f.originalID != (kadm.TopicID{}) && detail.ID != f.originalID {
		return fmt.Errorf("topic %q changed while reading", f.topic)
	}
	f.detail = detail
	return nil
}
