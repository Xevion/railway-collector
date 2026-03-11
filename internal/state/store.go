package state

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	logCursorBucket      = []byte("log_cursors")
	discoveryCacheBucket = []byte("discovery_cache")
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
		for _, b := range [][]byte{logCursorBucket, discoveryCacheBucket, coverageBucket} {
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

// GetDiscoveryCache returns the raw JSON for a cached workspace discovery cache entry, or nil if not found.
func (s *Store) GetDiscoveryCache(key string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(discoveryCacheBucket).Get([]byte(key))
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// SetDiscoveryCache stores raw JSON for a workspace discovery cache entry.
func (s *Store) SetDiscoveryCache(key string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(discoveryCacheBucket).Put([]byte(key), data)
	})
}

// ListDiscoveryCache returns all cached workspace discovery entries keyed by key.
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

// DeleteDiscoveryCache removes a cached workspace discovery cache entry.
func (s *Store) DeleteDiscoveryCache(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(discoveryCacheBucket).Delete([]byte(key))
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
