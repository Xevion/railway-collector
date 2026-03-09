package state

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	logCursorBucket    = []byte("log_cursors")
	metricCursorBucket = []byte("metric_cursors")
)

// Store persists collection cursors across restarts.
type Store struct {
	db *bolt.DB
}

// Open creates or opens a bbolt database at the given path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening state db %s: %w", path, err)
	}

	// Ensure buckets exist
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logCursorBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(metricCursorBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing state buckets: %w", err)
	}

	return &Store{db: db}, nil
}

// GetLogCursor returns the last-seen timestamp for a deployment+logType pair.
// Returns zero time if not found.
func (s *Store) GetLogCursor(deploymentID, logType string) time.Time {
	key := []byte(deploymentID + ":" + logType)
	var ts time.Time
	s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(logCursorBucket).Get(key)
		if v != nil {
			ts, _ = time.Parse(time.RFC3339Nano, string(v))
		}
		return nil
	})
	return ts
}

// SetLogCursor persists the last-seen timestamp for a deployment+logType pair.
// Only updates if the new timestamp is after the existing one.
func (s *Store) SetLogCursor(deploymentID, logType string, ts time.Time) error {
	key := []byte(deploymentID + ":" + logType)
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logCursorBucket)
		if existing := b.Get(key); existing != nil {
			if prev, err := time.Parse(time.RFC3339Nano, string(existing)); err == nil && !ts.After(prev) {
				return nil
			}
		}
		return b.Put(key, []byte(ts.Format(time.RFC3339Nano)))
	})
}

// GetMetricCursor returns the last metric fetch timestamp for a project.
// Returns zero time if not found.
func (s *Store) GetMetricCursor(projectID string) time.Time {
	key := []byte(projectID)
	var ts time.Time
	s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(metricCursorBucket).Get(key)
		if v != nil {
			ts, _ = time.Parse(time.RFC3339Nano, string(v))
		}
		return nil
	})
	return ts
}

// SetMetricCursor persists the last metric fetch timestamp for a project.
func (s *Store) SetMetricCursor(projectID string, ts time.Time) error {
	key := []byte(projectID)
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metricCursorBucket).Put(key, []byte(ts.Format(time.RFC3339Nano)))
	})
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}
