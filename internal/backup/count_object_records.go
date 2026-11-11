package backup

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

const recordCountCacheLimit = 100000

// CountObjectRecords counts the backup records (jsonl lines) in one object, decompressed by its file name.
type CountObjectRecords interface {
	Execute(ctx context.Context, object s3store.Object) (int64, error)
}

type countObjectRecords struct {
	get   s3store.GetObject
	mu    *sync.Mutex
	cache map[string]int64
}

func NewCountObjectRecords(get s3store.GetObject) CountObjectRecords {
	return countObjectRecords{get: get, mu: &sync.Mutex{}, cache: map[string]int64{}}
}

// Execute caches counts by key and size: backup objects are written once and never change.
func (c countObjectRecords) Execute(ctx context.Context, object s3store.Object) (int64, error) {
	cacheKey := object.Key + "\x00" + strconv.FormatInt(object.Size, 10)
	c.mu.Lock()
	count, ok := c.cache[cacheKey]
	c.mu.Unlock()
	if ok {
		return count, nil
	}
	body, err := c.get.Execute(ctx, object.Key)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	compression := BackupObjectCompression(object.Key)
	lines, err := OpenBackupObject(body, compression)
	if err != nil {
		return 0, fmt.Errorf("%w: %s is not %s: %w", ErrStorageUnavailable, object.Key, compression, err)
	}
	defer lines.Close()
	scanner := bufio.NewScanner(lines)
	scanner.Buffer(make([]byte, 64*1024), 64<<20)
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("%w: read %s: %w", ErrStorageUnavailable, object.Key, err)
	}
	c.mu.Lock()
	if len(c.cache) >= recordCountCacheLimit {
		clear(c.cache)
	}
	c.cache[cacheKey] = count
	c.mu.Unlock()
	return count, nil
}
