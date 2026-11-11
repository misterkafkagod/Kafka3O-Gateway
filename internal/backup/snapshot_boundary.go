package backup

import (
	"sort"
	"strconv"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

// SnapshotBoundary returns, for the topic's backup objects written with the compression, the highest start offset per
// partition as "partition:offset,..." (the restore transform's snapshot), the number of objects it covers and their
// partitions.
func SnapshotBoundary(objects []s3store.Object, topic, compression string) (string, int, int) {
	highest := map[int]int64{}
	count := 0
	for _, object := range objects {
		name, partition, offset, ok := ParseBackupObjectName(object.Key)
		if !ok || name != topic || BackupObjectCompression(object.Key) != compression {
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
