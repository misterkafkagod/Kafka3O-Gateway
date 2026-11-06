package storage

import (
	"net/url"
	"path/filepath"
	"runtime"
)

func sqliteDSN(path string) string {
	filePath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" {
		filePath = "/" + filePath
	}
	u := url.URL{Scheme: "file", Path: filePath}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Set("_defensive", "1")
	u.RawQuery = q.Encode()
	return u.String()
}
