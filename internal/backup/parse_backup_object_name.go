package backup

import (
	"path"
	"regexp"
	"strconv"
)

var backupObjectName = regexp.MustCompile(`^(.+)-(\d{1,10})-(\d{20})\.gz$`)

// ParseBackupObjectName reads the topic, partition and start offset from a backup object key's file name.
func ParseBackupObjectName(key string) (topic string, partition int, startOffset int64, ok bool) {
	match := backupObjectName.FindStringSubmatch(path.Base(key))
	if match == nil {
		return "", 0, 0, false
	}
	partition, err1 := strconv.Atoi(match[2])
	startOffset, err2 := strconv.ParseInt(match[3], 10, 64)
	if err1 != nil || err2 != nil {
		return "", 0, 0, false
	}
	return match[1], partition, startOffset, true
}
