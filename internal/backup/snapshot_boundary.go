package backup

import (
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

var backupObjectName = regexp.MustCompile(`^(.+)-(\d{1,10})-(\d{20})\.gz$`)

// SnapshotBoundary returns, for the topic's backup objects, the highest start offset per partition as
// "partition:offset,..." (the restore transform's snapshot), the number of objects it covers and their partitions.
func SnapshotBoundary(objects []s3store.Object, topic string) (string, int, int) {
	highest := map[int]int64{}
	count := 0
	for _, object := range objects {
		match := backupObjectName.FindStringSubmatch(path.Base(object.Key))
		if match == nil || match[1] != topic {
			continue
		}
		partition, err1 := strconv.Atoi(match[2])
		offset, err2 := strconv.ParseInt(match[3], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		count++
		if current, ok := highest[partition]; !ok || offset > current {
			highest[partition] = offset
		}
	}
	partitions := make([]int, 0, len(highest))
	for partition := range highest {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)
	pairs := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		pairs = append(pairs, strconv.Itoa(partition)+":"+strconv.FormatInt(highest[partition], 10))
	}
	return strings.Join(pairs, ","), count, len(partitions)
}
