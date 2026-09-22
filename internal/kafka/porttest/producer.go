package porttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// RunProducer exercises kafka.Producer (FUNC-SPEC §8.1, §8.7 M5). admin
// seeds the topic Producer writes to and reads it back for comparison; both
// are the same value for internal/kafka/fake, and a shared admin client at
// Level 2 (Task 16.2).
func RunProducer(t *testing.T, admin kafka.Admin, producer kafka.Producer) {
	t.Helper()

	t.Run("Producer_ProduceThenReadBackByteEqual", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-produce-readback", 1)

		results, err := producer.Produce(context.Background(), "t-produce-readback", []kafka.ProduceRequest{
			{Key: []byte("k1"), Value: []byte("v1"), Headers: []kafka.Header{{Key: "h", Value: []byte("hv")}}},
		})
		if err != nil {
			t.Fatalf("Produce() error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("Produce() results = %+v, want one successful result", results)
		}

		got, err := admin.DescribeTopics(context.Background(), "t-produce-readback")
		if err != nil {
			t.Fatalf("DescribeTopics() error: %v", err)
		}
		if len(got.Partitions) == 0 {
			t.Fatal("DescribeTopics() returned no partitions")
		}
		endOffsets, err := admin.ListEndOffsets(context.Background(), "t-produce-readback")
		if err != nil {
			t.Fatalf("ListEndOffsets() error: %v", err)
		}
		if endOffsets[results[0].Partition] != results[0].Offset+1 {
			t.Errorf("end offset for partition %d = %d, want %d", results[0].Partition, endOffsets[results[0].Partition], results[0].Offset+1)
		}
	})

	t.Run("Producer_ExplicitPartitionHonoured", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-produce-partition", 3)

		want := int32(2)
		results, err := producer.Produce(context.Background(), "t-produce-partition", []kafka.ProduceRequest{
			{Value: []byte("v"), Partition: &want},
		})
		if err != nil {
			t.Fatalf("Produce() error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("Produce() results = %+v, want one successful result", results)
		}
		if results[0].Partition != want {
			t.Errorf("Partition = %d, want %d", results[0].Partition, want)
		}
	})

	t.Run("Producer_PartitionOutOfRangeIsError", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-produce-outofrange", 1)

		outOfRange := int32(999)
		results, err := producer.Produce(context.Background(), "t-produce-outofrange", []kafka.ProduceRequest{
			{Value: []byte("v"), Partition: &outOfRange},
		})
		if err != nil {
			t.Fatalf("Produce() call-level error: %v", err)
		}
		if len(results) != 1 || results[0].Err == nil {
			t.Fatalf("Produce() results = %+v, want one record-level error", results)
		}
		var ke *kafka.Error
		if !errors.As(results[0].Err, &ke) {
			t.Errorf("results[0].Err = %v, want a *kafka.Error", results[0].Err)
		}
	})

	t.Run("Producer_TimestampPreservedOrNow", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-produce-timestamp", 1)

		explicit := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		before := time.Now()
		results, err := producer.Produce(context.Background(), "t-produce-timestamp", []kafka.ProduceRequest{
			{Value: []byte("explicit"), Timestamp: explicit},
			{Value: []byte("now")},
		})
		if err != nil {
			t.Fatalf("Produce() error: %v", err)
		}
		if len(results) != 2 || results[0].Err != nil || results[1].Err != nil {
			t.Fatalf("Produce() results = %+v, want two successful results", results)
		}
		if !results[0].Timestamp.Equal(explicit) {
			t.Errorf("explicit Timestamp = %v, want %v", results[0].Timestamp, explicit)
		}
		if results[1].Timestamp.Before(before) {
			t.Errorf("stamped-now Timestamp = %v, want at or after %v", results[1].Timestamp, before)
		}
	})
}
