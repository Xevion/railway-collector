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

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}
