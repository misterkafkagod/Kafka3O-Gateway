package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func finishTopicMessageRead(ctx context.Context, reader *kgo.Client, original kadm.TopicDetail, state *topicMessageReadState) error {
	decompressor, ok := reader.OptValue(kgo.WithDecompressor).(kgo.Decompressor)
	if !ok {
		return fmt.Errorf("topic reader has no decompressor")
	}
	fetcher := topicSnapshotFetcher{
		reader:       reader,
		admin:        kadm.NewClient(reader),
		topic:        state.topic,
		originalID:   original.ID,
		decompressor: decompressor,
	}
	if err := fetcher.refresh(ctx); err != nil {
		return err
	}
	return state.finish(ctx, fetcher.fetch)
}
