package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

type fakeLogDirsAdmin struct {
	dirs kadm.DescribedAllLogDirs
	err  error
}

func (f fakeLogDirsAdmin) DescribeAllLogDirs(context.Context, kadm.TopicsSet) (kadm.DescribedAllLogDirs, error) {
	return f.dirs, f.err
}

func TestReadLogDirs_ReportsVolumeAndKafkaSizes(t *testing.T) {
	admin := fakeLogDirsAdmin{dirs: kadm.DescribedAllLogDirs{1: {"/data": {
		Broker: 1, Dir: "/data", TotalBytes: 100, UsableBytes: 40,
		Topics: kadm.DescribedLogDirTopics{"orders": {0: {Size: 7}, 1: {Size: 5}}, "__consumer_offsets": {0: {Size: 3}}},
	}}}}
	dirs, err := NewReadLogDirs(admin).Execute(context.Background())
	if err != nil || len(dirs) != 1 {
		t.Fatalf("dirs = %+v, err = %v", dirs, err)
	}
	d := dirs[0]
	if d.Broker != 1 || d.Dir != "/data" || *d.TotalBytes != 100 || *d.UsableBytes != 40 || d.KafkaBytes != 15 || d.Error != "" {
		t.Fatalf("dir = %+v", d)
	}
}

func TestReadLogDirs_LeavesUnreportedSizesUnknownAndKeepsDirectoryErrors(t *testing.T) {
	admin := fakeLogDirsAdmin{dirs: kadm.DescribedAllLogDirs{1: {"/data": {Broker: 1, Dir: "/data", TotalBytes: -1, UsableBytes: -1, Err: errors.New("KAFKA_STORAGE_ERROR")}}}}
	dirs, _ := NewReadLogDirs(admin).Execute(context.Background())
	if dirs[0].TotalBytes != nil || dirs[0].UsableBytes != nil || dirs[0].Error != "KAFKA_STORAGE_ERROR" {
		t.Fatalf("dir = %+v", dirs[0])
	}
}

func TestReadLogDirs_KeepsPartialResultsWithTheRequestError(t *testing.T) {
	admin := fakeLogDirsAdmin{dirs: kadm.DescribedAllLogDirs{1: {"/data": {Broker: 1, Dir: "/data", TotalBytes: 1, UsableBytes: 1}}}, err: errors.New("broker 2 unreachable")}
	dirs, err := NewReadLogDirs(admin).Execute(context.Background())
	if len(dirs) != 1 || err == nil || !strings.Contains(err.Error(), "broker 2 unreachable") {
		t.Fatalf("dirs = %+v, err = %v", dirs, err)
	}
}
