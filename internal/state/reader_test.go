package state_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/state"
)

func setupTestDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	// Create with read-write store to seed data
	store, err := state.Open(path)
	require.NoError(t, err)

	// Seed discovery cache
	require.NoError(t, store.SetDiscoveryCache("proj-aaa", []byte(`{"targets":[],"expires_at":"2026-03-11T00:00:00Z"}`)))

	// Seed coverage
	require.NoError(t, store.SetCoverage("proj-aaa:metrics", []byte(`[{"start":"2026-03-09T00:00:00Z","end":"2026-03-10T00:00:00Z","kind":0}]`)))

	require.NoError(t, store.Close())
	return path
}

func TestReader_OpenReadOnly(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()
}

func TestReader_LogCursors(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()

	cursors, err := reader.LogCursors()
	require.NoError(t, err)
	assert.Empty(t, cursors)
}

func TestReader_DiscoveryEntries(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()

	entries, err := reader.DiscoveryEntries()
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "proj-aaa", entries[0].Key)
	assert.Contains(t, string(entries[0].Value), "targets")
}

func TestReader_CoverageEntries(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()

	entries, err := reader.CoverageEntries()
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "proj-aaa:metrics", entries[0].Key)
}

func TestReader_BucketStats(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()

	stats, err := reader.BucketStats()
	require.NoError(t, err)
	assert.Len(t, stats, 3)

	counts := map[string]int{}
	for _, s := range stats {
		counts[s.Name] = s.Count
	}
	assert.Equal(t, 0, counts["log_cursors"])
	assert.Equal(t, 1, counts["discovery_cache"])
	assert.Equal(t, 1, counts["coverage"])
}

func TestReader_DBFileSize(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadOnly(path)
	require.NoError(t, err)
	defer reader.Close()

	size, err := reader.DBFileSize()
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))
}

func TestReader_DeleteBucket(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadWrite(path)
	require.NoError(t, err)
	defer reader.Close()

	count, err := reader.DeleteBucket("discovery_cache")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify empty
	entries, err := reader.DiscoveryEntries()
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestReader_DeleteKey(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadWrite(path)
	require.NoError(t, err)
	defer reader.Close()

	err = reader.DeleteKey("discovery_cache", "proj-aaa")
	require.NoError(t, err)

	entries, err := reader.DiscoveryEntries()
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestReader_DeleteKey_NotFound(t *testing.T) {
	path := setupTestDB(t)
	reader, err := state.OpenReadWrite(path)
	require.NoError(t, err)
	defer reader.Close()

	err = reader.DeleteKey("log_cursors", "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestReader_OpenReadOnly_NonexistentPath(t *testing.T) {
	_, err := state.OpenReadOnly("/tmp/nonexistent-db-path-test.db")
	assert.Error(t, err)
}
