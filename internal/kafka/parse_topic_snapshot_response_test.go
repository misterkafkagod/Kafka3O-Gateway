package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTopicSnapshotFetcher_CompactedEmptyBatch(t *testing.T) {
	batch := kmsg.NewRecordBatch()
	batch.FirstOffset = 1
	batch.Magic = 2
	batch.LastOffsetDelta = 3
	raw := batch.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))

	id := kadm.TopicID{1}
	fetcher := topicSnapshotFetcher{
		topic:        "orders",
		detail:       kadm.TopicDetail{ID: id},
		decompressor: kgo.DefaultDecompressor(),
	}
	response := kmsg.NewPtrFetchResponse()
	response.Version = 13
	responseTopic := kmsg.NewFetchResponseTopic()
	responseTopic.TopicID = [16]byte(id)
	responsePartition := kmsg.NewFetchResponseTopicPartition()
	responsePartition.Partition = 0
	responsePartition.HighWatermark = 5
	responsePartition.RecordBatches = raw
	responseTopic.Partitions = []kmsg.FetchResponseTopicPartition{responsePartition}
	response.Topics = []kmsg.FetchResponseTopic{responseTopic}

	fetched, err := fetcher.parseResponse(response, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !fetched.hasBatch || fetched.next != 5 || len(fetched.partition.Records) != 0 {
		t.Fatalf("compacted batch = %#v", fetched)
	}
	state, err := newTopicMessageReadState("orders", map[int32]int64{0: 1}, map[int32]int64{0: 5}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.finish(context.Background(), func(context.Context, int32, int64) (fetchedTopicPartition, error) {
		return fetched, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !state.done[0] || state.messages == nil || len(state.messages) != 0 {
		t.Fatalf("compacted snapshot = %#v", state)
	}

	response.Topics[0].Partitions[0].RecordBatches = bytes.Clone(raw)
	response.Topics[0].Partitions[0].RecordBatches[21] ^= 1
	if _, err := fetcher.parseResponse(response, 0, 1); err == nil {
		t.Fatal("corrupt compacted batch was accepted")
	}
}
