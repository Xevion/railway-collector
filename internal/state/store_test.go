package state

import (
	"path/filepath"
	"testing"
	"time"

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

func TestStore_LogCursorBehavior(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		writes   []time.Time
		expected time.Time
	}{
		{"forward progression", []time.Time{t1, t2}, t2},
		{"regression blocked", []time.Time{t2, t1}, t2},
		{"same timestamp", []time.Time{t1, t1}, t1},
		{"multiple forward", []time.Time{t1, t2, t3}, t3},
		{"single write", []time.Time{t1}, t1},
		{"regression then forward", []time.Time{t3, t1, t2}, t3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)
			for _, ts := range tt.writes {
				require.NoError(t, store.SetLogCursor("dep-1", "env", ts))
			}
			got := store.GetLogCursor("dep-1", "env")
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestStore_MetricCursorRoundTrip(t *testing.T) {
	store := openTestStore(t)

	// Zero value when not set
	got := store.GetMetricCursor("proj-1")
	assert.True(t, got.IsZero(), "should return zero time when not set")

	// Set and retrieve
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.SetMetricCursor("proj-1", ts))
	got = store.GetMetricCursor("proj-1")
	assert.Equal(t, ts, got)

	// Overwrite (metric cursors don't have regression protection)
	ts2 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC) // earlier
	require.NoError(t, store.SetMetricCursor("proj-1", ts2))
	got = store.GetMetricCursor("proj-1")
	assert.Equal(t, ts2, got, "metric cursors should allow backward movement")

	// Different project keys are independent
	require.NoError(t, store.SetMetricCursor("proj-2", ts))
	got1 := store.GetMetricCursor("proj-1")
	got2 := store.GetMetricCursor("proj-2")
	assert.Equal(t, ts2, got1)
	assert.Equal(t, ts, got2)
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}
