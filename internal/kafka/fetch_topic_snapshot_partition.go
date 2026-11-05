package kafka

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (f *topicSnapshotFetcher) fetch(ctx context.Context, partition int32, offset int64) (fetchedTopicPartition, error) {
	delay := 100 * time.Millisecond
	for {
		result, err := f.fetchOnce(ctx, partition, offset)
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return fetchedTopicPartition{}, errors.Join(err, ctx.Err())
		}
		if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
			return fetchedTopicPartition{}, topicOffsetError(ctx, f.admin, f.topic, err)
		}
		var netErr *net.OpError
		if !kerr.IsRetriable(err) && !kgo.IsRetryableBrokerErr(err) && !(errors.As(err, &netErr) && netErr.Op == "dial") {
			return fetchedTopicPartition{}, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fetchedTopicPartition{}, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		if refreshErr := f.refresh(ctx); refreshErr != nil {
			return fetchedTopicPartition{}, errors.Join(err, refreshErr)
		}
		delay = min(delay*2, time.Second)
	}
}
