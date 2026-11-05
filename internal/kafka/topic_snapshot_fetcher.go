package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type topicSnapshotFetcher struct {
	reader       *kgo.Client
	admin        *kadm.Client
	topic        string
	originalID   kadm.TopicID
	detail       kadm.TopicDetail
	decompressor kgo.Decompressor
}
