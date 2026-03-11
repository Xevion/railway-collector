package state

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

// CursorEntry represents a single cursor stored in the database.
type CursorEntry struct {
	Key       string
	Timestamp time.Time
}

// RawEntry represents a raw key-value pair from a bucket.
type RawEntry struct {
	Key   string
	Value []byte
}

// BucketStat holds summary information about a single bucket.
type BucketStat struct {
	Name  string
	Count int
}

// Reader provides read-only access to the state database.
type Reader struct {
	db *bolt.DB
}

// OpenReadOnly opens the state database in read-only mode.
// Safe to call while the collector is running (uses shared flock).
func OpenReadOnly(path string) (*Reader, error) {
	db, err := bolt.Open(path, 0, &bolt.Options{
		ReadOnly: true,
		Timeout:  2 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening state db read-only %s: %w", path, err)
	}
	return &Reader{db: db}, nil
}

// OpenReadWrite opens the state database in read-write mode.
// Fails if the collector (or another writer) holds the lock.
func OpenReadWrite(path string) (*Reader, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: 2 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening state db %s (is the collector running?): %w", path, err)
	}
	return &Reader{db: db}, nil
}

// Close closes the underlying database.
func (r *Reader) Close() error {
	return r.db.Close()
}

// MetricCursors returns all entries from the metric_cursors bucket.
func (r *Reader) MetricCursors() ([]CursorEntry, error) {
	return r.readCursors(metricCursorBucket)
}

// LogCursors returns all entries from the log_cursors bucket.
func (r *Reader) LogCursors() ([]CursorEntry, error) {
	return r.readCursors(logCursorBucket)
}

func (r *Reader) readCursors(bucket []byte) ([]CursorEntry, error) {
	var entries []CursorEntry
	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			ts, _ := time.Parse(time.RFC3339Nano, string(v))
			entries = append(entries, CursorEntry{
				Key:       string(k),
				Timestamp: ts,
			})
			return nil
		})
	})
	return entries, err
}

// DiscoveryEntries returns all raw entries from the discovery_cache bucket.
func (r *Reader) DiscoveryEntries() ([]RawEntry, error) {
	return r.readRaw(discoveryCacheBucket)
}

// CoverageEntries returns all raw entries from the coverage bucket.
func (r *Reader) CoverageEntries() ([]RawEntry, error) {
	return r.readRaw(coverageBucket)
}

func (r *Reader) readRaw(bucket []byte) ([]RawEntry, error) {
	var entries []RawEntry
	err := r.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			val := make([]byte, len(v))
			copy(val, v)
			entries = append(entries, RawEntry{
				Key:   string(k),
				Value: val,
			})
			return nil
		})
	})
	return entries, err
}

// BucketStats returns entry counts for all known buckets.
func (r *Reader) BucketStats() ([]BucketStat, error) {
	buckets := [][]byte{
		metricCursorBucket,
		logCursorBucket,
		discoveryCacheBucket,
		coverageBucket,
	}

	var stats []BucketStat
	err := r.db.View(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			b := tx.Bucket(name)
			count := 0
			if b != nil {
				bStats := b.Stats()
				count = bStats.KeyN
			}
			stats = append(stats, BucketStat{
				Name:  string(name),
				Count: count,
			})
		}
		return nil
	})
	return stats, err
}

// DBFileSize returns the size of the database file in bytes.
func (r *Reader) DBFileSize() (int64, error) {
	info, err := os.Stat(r.db.Path())
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// DeleteBucket deletes all entries from a named bucket.
// Requires the database to be opened with OpenReadWrite.
func (r *Reader) DeleteBucket(bucketName string) (int, error) {
	name := []byte(bucketName)
	count := 0
	err := r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(name)
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		// Count entries first
		bStats := b.Stats()
		count = bStats.KeyN
		// Delete and recreate the bucket
		if err := tx.DeleteBucket(name); err != nil {
			return err
		}
		_, err := tx.CreateBucket(name)
		return err
	})
	return count, err
}

// DeleteKey deletes a single key from a named bucket.
// Requires the database to be opened with OpenReadWrite.
func (r *Reader) DeleteKey(bucketName, key string) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucketName)
		}
		if b.Get([]byte(key)) == nil {
			return fmt.Errorf("key %q not found in bucket %q", key, bucketName)
		}
		return b.Delete([]byte(key))
	})
}
