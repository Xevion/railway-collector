package state

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	logCursorBucket      = []byte("log_cursors")
	metricCursorBucket   = []byte("metric_cursors")
	discoveryCacheBucket = []byte("discovery_cache")
	projectListBucket    = []byte("project_list_cache")
	coverageBucket       = []byte("coverage")
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
		for _, b := range [][]byte{logCursorBucket, metricCursorBucket, discoveryCacheBucket, projectListBucket, coverageBucket} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
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

// GetDiscoveryCache returns the raw JSON for a cached project discovery entry, or nil if not found.
func (s *Store) GetDiscoveryCache(projectID string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(discoveryCacheBucket).Get([]byte(projectID))
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// SetDiscoveryCache stores raw JSON for a project discovery cache entry.
func (s *Store) SetDiscoveryCache(projectID string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(discoveryCacheBucket).Put([]byte(projectID), data)
	})
}

// ListDiscoveryCache returns all cached project discovery entries keyed by projectID.
func (s *Store) ListDiscoveryCache() (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(discoveryCacheBucket)
		return b.ForEach(func(k, v []byte) error {
			data := make([]byte, len(v))
			copy(data, v)
			result[string(k)] = data
			return nil
		})
	})
	return result, err
}

// DeleteDiscoveryCache removes a cached project discovery entry.
func (s *Store) DeleteDiscoveryCache(projectID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(discoveryCacheBucket).Delete([]byte(projectID))
	})
}

// GetProjectListCache returns the raw JSON for a cached project list, or nil if not found.
func (s *Store) GetProjectListCache(workspaceID string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(projectListBucket).Get([]byte(workspaceID))
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// SetProjectListCache stores raw JSON for a workspace's project list cache entry.
func (s *Store) SetProjectListCache(workspaceID string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(projectListBucket).Put([]byte(workspaceID), data)
	})
}

// GetCoverage returns the raw JSON coverage data for a key, or nil if not found.
func (s *Store) GetCoverage(key string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(coverageBucket).Get([]byte(key))
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// SetCoverage stores raw JSON coverage data for a key.
func (s *Store) SetCoverage(key string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(coverageBucket).Put([]byte(key), data)
	})
}

// ListCoverage returns all coverage entries keyed by their composite key.
func (s *Store) ListCoverage() (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(coverageBucket)
		return b.ForEach(func(k, v []byte) error {
			data := make([]byte, len(v))
			copy(data, v)
			result[string(k)] = data
			return nil
		})
	})
	return result, err
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}
