package metrics

import (
	"fmt"
	"slices"
	"testing"
)

func TestCompareLogDirs_OrdersByBrokerThenPath(t *testing.T) {
	dirs := []LogDir{{Broker: 2, Dir: "/a"}, {Broker: 1, Dir: "/b"}, {Broker: 1, Dir: "/a"}}
	slices.SortFunc(dirs, CompareLogDirs)
	var got []string
	for _, dir := range dirs {
		got = append(got, fmt.Sprintf("%d%s", dir.Broker, dir.Dir))
	}
	if !slices.Equal(got, []string{"1/a", "1/b", "2/a"}) {
		t.Fatalf("got %v", got)
	}
}
