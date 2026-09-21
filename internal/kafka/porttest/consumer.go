package porttest

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// commitAsserter is the optional capability internal/kafka/fake exposes to
// assert no commit or group-membership call was ever made (FUNC-SPEC O4).
type commitAsserter interface {
	AssertNoCommits(t *testing.T)
	AssertNoGroupJoin(t *testing.T)
}

// RunConsumer exercises kafka.Consumer (FUNC-SPEC §8.1, §9.2). admin
// resolves a timestamp into an offset for the case that needs it; consumer
// is the surface under test. For internal/kafka/fake both are the same
// value; at Level 2 (Task 16.2) they are a shared admin client and a
// dedicated per-scan consumer pointed at the same cluster.
func RunConsumer(t *testing.T, admin kafka.Admin, consumer kafka.Consumer) {
	t.Helper()

	t.Run("Consumer_AssignAndPollFromOffset", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-consumer-offset", 1,
			kafka.Record{Partition: 0, Value: []byte("a")},
			kafka.Record{Partition: 0, Value: []byte("b")},
			kafka.Record{Partition: 0, Value: []byte("c")},
		)

		if err := consumer.Assign(context.Background(), "t-consumer-offset", []int32{0}, map[int32]int64{0: 1}); err != nil {
			t.Fatalf("Assign() error: %v", err)
		}
		records, err := consumer.Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll() error: %v", err)
		}
		if len(records) != 2 || string(records[0].Value) != "b" || string(records[1].Value) != "c" {
			t.Fatalf("Poll() = %+v, want records b and c (skipping offset 0)", records)
		}
	})

	t.Run("Consumer_PollFromTimestampResolvedOffset", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		seeder.SeedTopic("t-consumer-timestamp", 1,
			kafka.Record{Partition: 0, Value: []byte("a"), Timestamp: base},
			kafka.Record{Partition: 0, Value: []byte("b"), Timestamp: base.Add(1 * time.Second)},
			kafka.Record{Partition: 0, Value: []byte("c"), Timestamp: base.Add(2 * time.Second)},
		)

		offsets, err := admin.ListOffsetsAfterMilli(context.Background(), "t-consumer-timestamp", base.Add(1*time.Second).UnixMilli())
		if err != nil {
			t.Fatalf("ListOffsetsAfterMilli() error: %v", err)
		}

		if err := consumer.Assign(context.Background(), "t-consumer-timestamp", []int32{0}, offsets); err != nil {
			t.Fatalf("Assign() error: %v", err)
		}
		records, err := consumer.Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll() error: %v", err)
		}
		if len(records) != 2 || string(records[0].Value) != "b" || string(records[1].Value) != "c" {
			t.Fatalf("Poll() from the resolved timestamp offset = %+v, want b and c", records)
		}
	})

	t.Run("Consumer_BeyondEndReturnsNothing", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-consumer-beyond", 1, kafka.Record{Partition: 0, Value: []byte("a")})

		if err := consumer.Assign(context.Background(), "t-consumer-beyond", []int32{0}, map[int32]int64{0: 999}); err != nil {
			t.Fatalf("Assign() error: %v", err)
		}
		records, err := consumer.Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll() error: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("Poll() beyond the end = %+v, want none", records)
		}
	})

	t.Run("Consumer_NoCommitNoGroupJoin", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		asserter, ok2 := consumer.(commitAsserter)
		if !ok || !ok2 {
			t.Skip("port does not implement the seeding/assertion capability")
		}
		seeder.SeedTopic("t-consumer-nocommit", 1, kafka.Record{Partition: 0, Value: []byte("a")})

		if err := consumer.Assign(context.Background(), "t-consumer-nocommit", []int32{0}, map[int32]int64{0: 0}); err != nil {
			t.Fatalf("Assign() error: %v", err)
		}
		if _, err := consumer.Poll(context.Background()); err != nil {
			t.Fatalf("Poll() error: %v", err)
		}

		asserter.AssertNoCommits(t)
		asserter.AssertNoGroupJoin(t)
	})

	t.Run("Consumer_CloseReleases", func(t *testing.T) {
		seeder, ok := admin.(topicSeeder)
		if !ok {
			t.Skip("admin does not implement the topic seeding capability")
		}
		seeder.SeedTopic("t-consumer-close", 1, kafka.Record{Partition: 0, Value: []byte("a")})

		if err := consumer.Assign(context.Background(), "t-consumer-close", []int32{0}, map[int32]int64{0: 0}); err != nil {
			t.Fatalf("Assign() error: %v", err)
		}
		if _, err := consumer.Poll(context.Background()); err != nil {
			t.Fatalf("Poll() error: %v", err)
		}
		consumer.Close()
	})
}
