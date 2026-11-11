package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

const scanCacheLimit = 100000

// ScanObjectRecords reads one backup object (decompressed by its file name) and reports, for a window, how many
// records it holds and the line number of the last one; AllRecords counts lines without parsing them.
type ScanObjectRecords interface {
	Execute(ctx context.Context, object s3store.Object, window RecordWindow) (ObjectScan, error)
}

type scanObjectRecords struct {
	get   s3store.GetObject
	mu    *sync.Mutex
	cache map[string]ObjectScan
}

func NewScanObjectRecords(get s3store.GetObject) ScanObjectRecords {
	return scanObjectRecords{get: get, mu: &sync.Mutex{}, cache: map[string]ObjectScan{}}
}

// Execute caches scans by key, size and window: backup objects are written once and never change.
func (s scanObjectRecords) Execute(ctx context.Context, object s3store.Object, window RecordWindow) (ObjectScan, error) {
	cacheKey := object.Key + "\x00" + strconv.FormatInt(object.Size, 10) + "\x00" + strconv.FormatInt(window.From, 10) + "\x00" + strconv.FormatInt(window.To, 10)
	s.mu.Lock()
	scan, ok := s.cache[cacheKey]
	s.mu.Unlock()
	if ok {
		return scan, nil
	}
	body, err := s.get.Execute(ctx, object.Key)
	if err != nil {
		return ObjectScan{}, err
	}
	defer body.Close()
	compression := BackupObjectCompression(object.Key)
	lines, err := OpenBackupObject(body, compression)
	if err != nil {
		return ObjectScan{}, fmt.Errorf("%w: %s is not %s: %w", ErrStorageUnavailable, object.Key, compression, err)
	}
	defer lines.Close()
	scanner := bufio.NewScanner(lines)
	scanner.Buffer(make([]byte, 64*1024), 64<<20)
	var line int64
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		line++
		if window != AllRecords {
			var record struct {
				Timestamp string `json:"timestamp"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return ObjectScan{}, fmt.Errorf("%w: %s line %d is not a backup record", ErrStorageUnavailable, object.Key, line)
			}
			at, err := time.Parse(time.RFC3339Nano, record.Timestamp)
			if err != nil {
				return ObjectScan{}, fmt.Errorf("%w: %s line %d has no valid timestamp", ErrStorageUnavailable, object.Key, line)
			}
			if millis := at.UnixMilli(); millis < window.From || millis >= window.To {
				continue
			}
		}
		scan.Matched++
		scan.LastMatch = line
	}
	if err := scanner.Err(); err != nil {
		return ObjectScan{}, fmt.Errorf("%w: read %s: %w", ErrStorageUnavailable, object.Key, err)
	}
	s.mu.Lock()
	if len(s.cache) >= scanCacheLimit {
		clear(s.cache)
	}
	s.cache[cacheKey] = scan
	s.mu.Unlock()
	return scan, nil
}
