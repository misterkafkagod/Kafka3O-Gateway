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

var readAt = time.Date(2026, 11, 10, 12, 0, 0, 0, time.FixedZone("x", 3600))

func TestGetMetrics_SortsLogDirsAndStampsTheReadTime(t *testing.T) {
	reader := &fakeReadLogDirs{dirs: []LogDir{{Broker: 2, Dir: "/data"}, {Broker: 1, Dir: "/data"}}}
	got := NewGetMetrics(reader, time.Second, func() time.Time { return readAt }).Execute(context.Background())
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
	got := NewGetMetrics(&fakeReadLogDirs{err: errors.New("CLUSTER_AUTHORIZATION_FAILED")}, time.Second, time.Now).Execute(context.Background())
	if got.LogDirs.Error != "CLUSTER_AUTHORIZATION_FAILED" || got.LogDirs.Dirs == nil || len(got.LogDirs.Dirs) != 0 {
		t.Fatalf("logDirs = %+v", got.LogDirs)
	}
}

func TestGetMetrics_KeepsPartialResultsWithTheError(t *testing.T) {
	got := NewGetMetrics(&fakeReadLogDirs{dirs: []LogDir{{Broker: 1, Dir: "/data"}}, err: errors.New("broker 2: timeout")}, time.Second, time.Now).Execute(context.Background())
	if got.LogDirs.Error != "broker 2: timeout" || len(got.LogDirs.Dirs) != 1 {
		t.Fatalf("logDirs = %+v", got.LogDirs)
	}
}
