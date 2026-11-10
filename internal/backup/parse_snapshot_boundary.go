package backup

import (
	"strconv"
	"strings"
)

// ParseSnapshotBoundary reads "partition:offset,..." back into the highest start offset per partition.
func ParseSnapshotBoundary(boundary string) (map[int]int64, bool) {
	highest := map[int]int64{}
	if boundary == "" {
		return highest, false
	}
	for _, pair := range strings.Split(boundary, ",") {
		partition, offset, found := strings.Cut(strings.TrimSpace(pair), ":")
		p, err1 := strconv.Atoi(partition)
		o, err2 := strconv.ParseInt(offset, 10, 64)
		if !found || err1 != nil || err2 != nil {
			return nil, false
		}
		highest[p] = o
	}
	return highest, true
}
