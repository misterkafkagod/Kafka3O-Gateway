package backup

import (
	"path"
	"strings"
)

// BackupObjectCompression returns the compression a backup object was written with, from its file name's extension.
func BackupObjectCompression(key string) string {
	switch name := path.Base(key); {
	case strings.HasSuffix(name, ".gz"):
		return "gzip"
	case strings.HasSuffix(name, ".zst"):
		return "zstd"
	case strings.HasSuffix(name, ".snappy"):
		return "snappy"
	default:
		return "none"
	}
}
