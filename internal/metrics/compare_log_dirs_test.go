package metrics

import (
	"slices"
	"testing"
)

func TestCompareLogDirs_OrdersByBrokerThenPath(t *testing.T) {
	dirs := []LogDir{{Broker: 2, Dir: "/a"}, {Broker: 1, Dir: "/b"}, {Broker: 1, Dir: "/a"}}
	slices.SortFunc(dirs, CompareLogDirs)
	want := []LogDir{{Broker: 1, Dir: "/a"}, {Broker: 1, Dir: "/b"}, {Broker: 2, Dir: "/a"}}
	if !slices.Equal(dirs, want) {
		t.Fatalf("got %v", dirs)
	}
}
