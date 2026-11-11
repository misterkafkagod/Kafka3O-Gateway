package metrics

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeReadLogDirs struct {
	dirs     []LogDir
	err      error
	deadline bool
}

func (f *fakeReadLogDirs) Execute(ctx context.Context) ([]LogDir, error) {
	_, f.deadline = ctx.Deadline()
	return f.dirs, f.err
}

type fakeReadPartitionHealth struct {
	topics []TopicHealth
	err    error
}

func (f fakeReadPartitionHealth) Execute(context.Context) ([]TopicHealth, error) {
	return f.topics, f.err
}

type fakeReadEndOffsets struct {
	err   error
	asked []string
}

func (f *fakeReadEndOffsets) Execute(_ context.Context, topics []string) ([]TopicEndOffset, error) {
	f.asked = topics
	if f.err != nil {
		return nil, f.err
	}
	return []TopicEndOffset{{Topic: "orders", EndOffset: "42"}}, nil
}

type fakeReadQuorum struct {
	quorum Quorum
	err    error
}

func (f fakeReadQuorum) Execute(context.Context) (Quorum, error) {
	return f.quorum, f.err
}

var healthy = fakeReadPartitionHealth{topics: []TopicHealth{{Topic: "orders", Partitions: 3, BelowMinISR: new(int)}}}

var readAt = time.Date(2026, 11, 10, 12, 0, 0, 0, time.FixedZone("x", 3600))

func TestGetMetrics_SortsLogDirsAndStampsTheReadTime(t *testing.T) {
	reader := &fakeReadLogDirs{dirs: []LogDir{{Broker: 2, Dir: "/data"}, {Broker: 1, Dir: "/data"}}}
	got := NewGetMetrics(Sources{LogDirs: reader, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{}}, time.Second, func() time.Time { return readAt }).Execute(context.Background())
	if !got.ReadAt.Equal(readAt) || got.ReadAt.Location() != time.UTC {
		t.Fatalf("readAt = %v", got.ReadAt)
	}
	if got.LogDirs.Error != "" || got.LogDirs.Dirs[0].Broker != 1 || got.LogDirs.Dirs[1].Broker != 2 {
		t.Fatalf("logDirs = %+v", got.LogDirs)
	}
	if !reader.deadline {
		t.Fatal("the Kafka read had no timeout")
	}
}

func TestGetMetrics_ReportsAFailedReadInItsSection(t *testing.T) {
	got := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{err: errors.New("CLUSTER_AUTHORIZATION_FAILED")}, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if got.LogDirs.Error != "CLUSTER_AUTHORIZATION_FAILED" || got.LogDirs.Dirs == nil || len(got.LogDirs.Dirs) != 0 {
		t.Fatalf("logDirs = %+v", got.LogDirs)
	}
	if got.LargestTopics.Error != "CLUSTER_AUTHORIZATION_FAILED" || got.LargestTopics.Topics == nil || got.Partitions.Partitions != 3 {
		t.Fatalf("largest = %+v, partitions = %+v", got.LargestTopics, got.Partitions)
	}
}

func TestGetMetrics_KeepsPartialResultsWithTheError(t *testing.T) {
	got := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{dirs: []LogDir{{Broker: 1, Dir: "/data"}}, err: errors.New("broker 2: timeout")}, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if got.LogDirs.Error != "broker 2: timeout" || len(got.LogDirs.Dirs) != 1 {
		t.Fatalf("logDirs = %+v", got.LogDirs)
	}
}

func TestGetMetrics_AddsPartitionHealthAndLargestTopics(t *testing.T) {
	reader := &fakeReadLogDirs{dirs: []LogDir{{Broker: 1, Dir: "/data", TopicBytes: map[string]int64{"orders": 9, "__consumer_offsets": 4}}}}
	got := NewGetMetrics(Sources{LogDirs: reader, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if got.Partitions.Topics != 1 || got.Partitions.Partitions != 3 || *got.Partitions.BelowMinISR != 0 {
		t.Fatalf("partitions = %+v", got.Partitions)
	}
	if len(got.LargestTopics.Topics) != 2 || got.LargestTopics.Topics[0] != (TopicSize{"orders", 9}) || got.LargestTopics.Error != "" {
		t.Fatalf("largest = %+v", got.LargestTopics)
	}
}

func TestGetMetrics_ReportsAFailedHealthReadInItsSection(t *testing.T) {
	got := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{}, Health: fakeReadPartitionHealth{err: errors.New("read topic metadata: refused")}, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if got.Partitions.Error != "read topic metadata: refused" || got.Partitions.BelowMinISR != nil || got.Partitions.UnreadableTopics == nil {
		t.Fatalf("partitions = %+v", got.Partitions)
	}
	if got.EndOffsets.Error != "read topic metadata: refused" || got.EndOffsets.Topics == nil {
		t.Fatalf("endOffsets = %+v", got.EndOffsets)
	}
}

func TestGetMetrics_ReadsEndOffsetsForReadableTopics(t *testing.T) {
	offsets := &fakeReadEndOffsets{}
	health := fakeReadPartitionHealth{topics: []TopicHealth{{Topic: "orders", BelowMinISR: new(int)}, {Topic: "secret", Error: "denied"}}}
	got := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{}, Health: health, Offsets: offsets, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if len(offsets.asked) != 1 || offsets.asked[0] != "orders" || got.EndOffsets.Topics[0].EndOffset != "42" || got.EndOffsets.Error != "" {
		t.Fatalf("asked = %v, endOffsets = %+v", offsets.asked, got.EndOffsets)
	}
	failed := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{}, Health: health, Offsets: &fakeReadEndOffsets{err: errors.New("list end offsets: refused")}, Quorum: fakeReadQuorum{}}, time.Second, time.Now).Execute(context.Background())
	if failed.EndOffsets.Error != "list end offsets: refused" || failed.EndOffsets.Topics == nil {
		t.Fatalf("endOffsets = %+v", failed.EndOffsets)
	}
}

func TestGetMetrics_Quorum(t *testing.T) {
	leader := int32(1)
	quorum := Quorum{LeaderID: &leader, LeaderEpoch: 3, HighWatermark: 9, Replicas: []QuorumReplica{{ID: 1, Role: "voter"}}}
	got := NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{}, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{quorum: quorum}}, time.Second, time.Now).Execute(context.Background())
	if got.Quorum.Error != "" || *got.Quorum.LeaderID != 1 || len(got.Quorum.Replicas) != 1 {
		t.Fatalf("quorum = %+v", got.Quorum)
	}
	got = NewGetMetrics(Sources{LogDirs: &fakeReadLogDirs{}, Health: healthy, Offsets: &fakeReadEndOffsets{}, Quorum: fakeReadQuorum{err: errors.New("describe quorum: CLUSTER_AUTHORIZATION_FAILED")}}, time.Second, time.Now).Execute(context.Background())
	if got.Quorum.Error != "describe quorum: CLUSTER_AUTHORIZATION_FAILED" || got.Quorum.LeaderID != nil || got.Quorum.Replicas == nil || got.Partitions.Error != "" {
		t.Fatalf("a quorum failure must stay in its section: %+v", got.Quorum)
	}
}
