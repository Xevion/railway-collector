package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_CoverageRoundTrip(t *testing.T) {
	store := openTestStore(t)

	key := "proj-1:svc-1:env-1:metric:MEMORY_USAGE_GB"
	data := []byte(`[{"start":"2026-03-01T00:00:00Z","end":"2026-03-01T01:00:00Z","kind":0,"resolution":30}]`)

	err := store.SetCoverage(key, data)
	require.NoError(t, err)

	got, err := store.GetCoverage(key)
	require.NoError(t, err)
	assert.JSONEq(t, string(data), string(got))
}

func TestStore_CoverageNotFound(t *testing.T) {
	store := openTestStore(t)

	got, err := store.GetCoverage("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_DiscoveryCacheRoundTrip(t *testing.T) {
	store := openTestStore(t)

	key := "workspace-abc"
	data := []byte(`{"projects":[{"id":"proj-1","name":"my-project"}]}`)

	err := store.SetDiscoveryCache(key, data)
	require.NoError(t, err)

	got, err := store.GetDiscoveryCache(key)
	require.NoError(t, err)
	assert.JSONEq(t, string(data), string(got))
}

func TestStore_DiscoveryCacheNotFound(t *testing.T) {
	store := openTestStore(t)

	got, err := store.GetDiscoveryCache("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_DiscoveryCacheOverwrite(t *testing.T) {
	store := openTestStore(t)

	key := "workspace-abc"
	first := []byte(`{"projects":[{"id":"proj-1","name":"old"}]}`)
	second := []byte(`{"projects":[{"id":"proj-1","name":"new"}]}`)

	err := store.SetDiscoveryCache(key, first)
	require.NoError(t, err)

	err = store.SetDiscoveryCache(key, second)
	require.NoError(t, err)

	got, err := store.GetDiscoveryCache(key)
	require.NoError(t, err)
	assert.JSONEq(t, string(second), string(got))
}

func TestStore_ListDiscoveryCache(t *testing.T) {
	store := openTestStore(t)

	data1 := []byte(`{"projects":[{"id":"proj-1"}]}`)
	data2 := []byte(`{"projects":[{"id":"proj-2"}]}`)

	require.NoError(t, store.SetDiscoveryCache("workspace-1", data1))
	require.NoError(t, store.SetDiscoveryCache("workspace-2", data2))

	result, err := store.ListDiscoveryCache()
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.JSONEq(t, string(data1), string(result["workspace-1"]))
	assert.JSONEq(t, string(data2), string(result["workspace-2"]))
}

func TestStore_ListDiscoveryCache_Empty(t *testing.T) {
	store := openTestStore(t)

	result, err := store.ListDiscoveryCache()
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestStore_DeleteDiscoveryCache(t *testing.T) {
	store := openTestStore(t)

	key := "workspace-abc"
	data := []byte(`{"projects":[{"id":"proj-1"}]}`)

	require.NoError(t, store.SetDiscoveryCache(key, data))

	err := store.DeleteDiscoveryCache(key)
	require.NoError(t, err)

	got, err := store.GetDiscoveryCache(key)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_DeleteDiscoveryCache_Nonexistent(t *testing.T) {
	store := openTestStore(t)

	err := store.DeleteDiscoveryCache("nonexistent")
	require.NoError(t, err)
}

func TestStore_CoverageOverwrite(t *testing.T) {
	store := openTestStore(t)

	key := "proj-1:svc-1:env-1:metric:CPU_USAGE"
	first := []byte(`[{"start":"2026-03-01T00:00:00Z","end":"2026-03-01T01:00:00Z","kind":0,"resolution":30}]`)
	second := []byte(`[{"start":"2026-03-01T00:00:00Z","end":"2026-03-01T02:00:00Z","kind":0,"resolution":30}]`)

	require.NoError(t, store.SetCoverage(key, first))
	require.NoError(t, store.SetCoverage(key, second))

	got, err := store.GetCoverage(key)
	require.NoError(t, err)
	assert.JSONEq(t, string(second), string(got))
}

func TestStore_ListCoverage(t *testing.T) {
	store := openTestStore(t)

	key1 := "proj-1:svc-1:env-1:metric:MEMORY_USAGE_GB"
	key2 := "proj-1:svc-1:env-1:metric:CPU_USAGE"
	data1 := []byte(`[{"start":"2026-03-01T00:00:00Z","end":"2026-03-01T01:00:00Z","kind":0,"resolution":30}]`)
	data2 := []byte(`[{"start":"2026-03-02T00:00:00Z","end":"2026-03-02T01:00:00Z","kind":0,"resolution":60}]`)

	require.NoError(t, store.SetCoverage(key1, data1))
	require.NoError(t, store.SetCoverage(key2, data2))

	result, err := store.ListCoverage()
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.JSONEq(t, string(data1), string(result[key1]))
	assert.JSONEq(t, string(data2), string(result[key2]))
}

func TestStore_ListCoverage_Empty(t *testing.T) {
	store := openTestStore(t)

	result, err := store.ListCoverage()
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestStore_Open_InvalidPath verifies that Open returns an error when the path
// cannot be created (parent directory does not exist).
func TestStore_Open_InvalidPath(t *testing.T) {
	_, err := Open("/nonexistent/deeply/nested/path/test.db")
	assert.Error(t, err)
}

// TestStore_OperationsOnClosed verifies that calling operations on a closed store
// returns an error. bbolt returns ErrDatabaseNotOpen rather than panicking when
// View/Update are called on a closed *bolt.DB.
func TestStore_OperationsOnClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.db")
	store, err := Open(path)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	// Both read and write paths should return errors on a closed DB.
	_, err = store.GetDiscoveryCache("any-key")
	assert.Error(t, err, "GetDiscoveryCache on closed store should return error")

	err = store.SetDiscoveryCache("any-key", []byte(`{}`))
	assert.Error(t, err, "SetDiscoveryCache on closed store should return error")
}

// TestStore_ListCoverage_MultipleEntries verifies that ListCoverage returns all
// inserted entries when there are 3 or more, not just the first.
func TestStore_ListCoverage_MultipleEntries(t *testing.T) {
	store := openTestStore(t)

	entries := map[string][]byte{
		"proj-1:svc-1:env-1:metric:CPU":    []byte(`[{"start":"2026-03-01T00:00:00Z","end":"2026-03-01T01:00:00Z","kind":0,"resolution":30}]`),
		"proj-1:svc-1:env-1:metric:MEMORY": []byte(`[{"start":"2026-03-01T01:00:00Z","end":"2026-03-01T02:00:00Z","kind":0,"resolution":30}]`),
		"proj-2:svc-2:env-2:metric:DISK":   []byte(`[{"start":"2026-03-01T02:00:00Z","end":"2026-03-01T03:00:00Z","kind":0,"resolution":60}]`),
	}

	for key, data := range entries {
		require.NoError(t, store.SetCoverage(key, data))
	}

	result, err := store.ListCoverage()
	require.NoError(t, err)
	require.Len(t, result, 3, "all three entries should be returned")

	for key, data := range entries {
		assert.JSONEq(t, string(data), string(result[key]))
	}
}

// TestStore_ListDiscoveryCache_MultipleEntries verifies that ListDiscoveryCache returns
// all inserted entries when there are 3 or more, not just the first.
func TestStore_ListDiscoveryCache_MultipleEntries(t *testing.T) {
	store := openTestStore(t)

	entries := map[string][]byte{
		"workspace-alpha": []byte(`{"projects":[{"id":"proj-1"}]}`),
		"workspace-beta":  []byte(`{"projects":[{"id":"proj-2"}]}`),
		"workspace-gamma": []byte(`{"projects":[{"id":"proj-3"}]}`),
	}

	for key, data := range entries {
		require.NoError(t, store.SetDiscoveryCache(key, data))
	}

	result, err := store.ListDiscoveryCache()
	require.NoError(t, err)
	require.Len(t, result, 3, "all three entries should be returned")

	for key, data := range entries {
		assert.JSONEq(t, string(data), string(result[key]))
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}
